package eu.kanade.domain.source.service

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import dev.zacsweers.metro.AppScope
import dev.zacsweers.metro.Inject
import dev.zacsweers.metro.SingleIn
import eu.kanade.domain.manga.model.toSManga
import eu.kanade.domain.source.service.SourcePreferences
import eu.kanade.tachiyomi.source.ProgressAwareSource
import eu.kanade.tachiyomi.source.model.ChapterProgress
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.util.system.workManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import logcat.LogPriority
import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.chapter.interactor.UpdateChapter
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.chapter.model.ChapterUpdate
import tachiyomi.domain.manga.interactor.GetManga
import tachiyomi.domain.source.service.SourceManager
import tachiyomi.domain.source.service.SourceProgressPolicy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Host coordinator for [ProgressAwareSource]: offline queue, pull merge, retry.
 */
@Inject
@SingleIn(AppScope::class)
class SourceProgressSync(
    private val updateChapter: UpdateChapter,
    private val preferenceStore: PreferenceStore,
    private val getManga: GetManga,
    private val sourceManager: SourceManager,
    private val context: Context,
    private val sourcePreferences: SourcePreferences,
) {
    data class ReaderPullResult(val pageIndex: Int, val completed: Boolean)

    @Volatile
    internal var flushTimeoutMs: Long = FLUSH_TIMEOUT_MS

    @Volatile
    internal var seriesTimeoutMs: Long = SERIES_TIMEOUT_MS

    /** Overridable in tests; null means read from preferences. */
    @Volatile
    internal var progressPullTtlMsOverride: Long? = null

    private val queueMutex = Mutex()
    private val pendingPush = ConcurrentHashMap<Long, ConcurrentHashMap<String, StoredProgress>>()
    private val pendingSourceId = ConcurrentHashMap<Long, Long>()
    private val flushInFlight = ConcurrentHashMap.newKeySet<Long>()
    private val flushDirty = ConcurrentHashMap.newKeySet<Long>()
    private val syncedMangaIds = ConcurrentHashMap.newKeySet<Long>()
    private val lastProgressPullAt = ConcurrentHashMap<Long, Long>()
    /** Local progress change clocks (mangaId -> chapterUrl -> epoch ms). */
    private val progressClocks = ConcurrentHashMap<Long, ConcurrentHashMap<String, Long>>()
    private val backoffUntil = ConcurrentHashMap<Long, Long>()
    private val backoffAttempt = ConcurrentHashMap<Long, Int>()
    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loaded = false

    private val queuePref by lazy {
        preferenceStore.getString(Preference.privateKey(QUEUE_PREF_KEY), "{}")
    }

    private val syncedMangaPref by lazy {
        preferenceStore.getStringSet(Preference.privateKey("source_progress_synced_manga"), emptySet())
    }

    private val sourceIdPref by lazy {
        preferenceStore.getString(Preference.privateKey(SOURCE_ID_PREF_KEY), "{}")
    }

    private suspend fun ensureLoaded() {
        queueMutex.withLock {
            if (loaded) return
            try {
                val raw = queuePref.get()
                if (raw.isNotBlank() && raw != "{}") {
                    val parsed = json.decodeFromString<Map<String, Map<String, StoredProgress>>>(raw)
                    for ((mangaIdStr, chapters) in parsed) {
                        val mangaId = mangaIdStr.toLongOrNull() ?: continue
                        val map = pendingPush.getOrPut(mangaId) { ConcurrentHashMap() }
                        for ((url, stored) in chapters) {
                            map[url] = stored
                        }
                    }
                }
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "$LOG_TAG failed to load offline queue" }
            }
            try {
                syncedMangaIds.clear()
                syncedMangaIds.addAll(syncedMangaPref.get().mapNotNull { it.toLongOrNull() })
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "$LOG_TAG failed to load synced manga set" }
            }
            try {
                val raw = sourceIdPref.get()
                if (raw.isNotBlank() && raw != "{}") {
                    val parsed = json.decodeFromString<Map<String, Long>>(raw)
                    for ((mangaIdStr, sourceId) in parsed) {
                        val mangaId = mangaIdStr.toLongOrNull() ?: continue
                        pendingSourceId[mangaId] = sourceId
                    }
                }
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "$LOG_TAG failed to load source id map" }
            }
            loaded = true
        }
    }

    private fun hasCompletedInitialSync(mangaId: Long): Boolean = mangaId in syncedMangaIds

    private suspend fun markInitialSyncDone(mangaId: Long) {
        if (mangaId in syncedMangaIds) return
        queueMutex.withLock {
            if (mangaId in syncedMangaIds) return
            syncedMangaIds.add(mangaId)
            syncedMangaPref.set(syncedMangaIds.map { it.toString() }.toSet())
        }
    }

    private fun hasPending(mangaId: Long, chapterUrl: String): Boolean {
        return pendingPush[mangaId]?.containsKey(chapterUrl) == true
    }

    fun hasPendingQueue(): Boolean = pendingPush.any { it.value.isNotEmpty() }

    fun hasPendingQueue(mangaId: Long): Boolean = pendingPush[mangaId]?.isNotEmpty() == true

    private fun progressPullTtlMs(): Long {
        progressPullTtlMsOverride?.let { return it }
        val seconds = sourcePreferences.sourceProgressPullTtlSeconds.get().toIntOrNull() ?: 60
        return seconds.coerceAtLeast(0) * 1000L
    }

    private fun isFresh(mangaId: Long): Boolean {
        val ttl = progressPullTtlMs()
        if (ttl <= 0L) return false
        val at = lastProgressPullAt[mangaId] ?: return false
        return System.currentTimeMillis() - at < ttl
    }

    private fun markProgressPullSuccess(mangaId: Long) {
        lastProgressPullAt[mangaId] = System.currentTimeMillis()
    }

    private fun localProgressClock(mangaId: Long, chapterUrl: String): Long =
        progressClocks[mangaId]?.get(chapterUrl) ?: 0L

    private fun noteLocalProgress(mangaId: Long, chapterUrl: String, at: Long = System.currentTimeMillis()) {
        progressClocks.getOrPut(mangaId) { ConcurrentHashMap() }[chapterUrl] = at
    }

    private fun noteRemoteProgress(mangaId: Long, chapterUrl: String, remoteLastModified: Long) {
        if (remoteLastModified > 0L) {
            progressClocks.getOrPut(mangaId) { ConcurrentHashMap() }[chapterUrl] = remoteLastModified
        }
    }


    private suspend fun persistQueue() {
        val snapshot = pendingPush.mapKeys { it.key.toString() }.mapValues { (_, chapters) ->
            chapters.mapValues { it.value }
        }
        queuePref.set(json.encodeToString(snapshot))
        sourceIdPref.set(json.encodeToString(pendingSourceId.mapKeys { it.key.toString() }))
    }

    suspend fun enqueue(mangaId: Long, sourceId: Long, progress: ChapterProgress) {
        enqueueAll(mangaId, sourceId, listOf(progress))
    }

    suspend fun enqueueAll(mangaId: Long, sourceId: Long, progress: List<ChapterProgress>) {
        if (progress.isEmpty()) return
        ensureLoaded()
        val now = System.currentTimeMillis()
        queueMutex.withLock {
            pendingSourceId[mangaId] = sourceId
            val map = pendingPush.getOrPut(mangaId) { ConcurrentHashMap() }
            for (p in progress) {
                val existing = map[p.chapterUrl]
                map[p.chapterUrl] = StoredProgress.from(p, existing, now)
            }
            persistQueue()
        }
    }

    suspend fun pullForReaderOpen(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapter: Chapter,
        localPageIndex: Int,
        localCompleted: Boolean,
        sourceId: Long,
        timeoutMs: Long = READER_TIMEOUT_MS,
        force: Boolean = false,
    ): ReaderPullResult {
        ensureLoaded()

        if (hasPending(mangaId, chapter.url)) {
            flushAsync(source, manga, mangaId)
            return ReaderPullResult(localPageIndex, localCompleted)
        }

        if (!force && isFresh(mangaId)) {
            return ReaderPullResult(localPageIndex, localCompleted)
        }

        data class PullOutcome(val remote: ChapterProgress?, val failed: Boolean)

        val outcome = withTimeoutOrNull(timeoutMs) {
            try {
                val list = source.getProgress(manga, listOf(chapter.url))
                PullOutcome(
                    remote = list.firstOrNull { it.chapterUrl == chapter.url },
                    failed = false,
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logcat(LogPriority.WARN) {
                    "$LOG_TAG getProgress reader manga=$mangaId: ${e.message}"
                }
                PullOutcome(null, failed = true)
            }
        } ?: PullOutcome(null, failed = true)

        if (outcome.failed) return ReaderPullResult(localPageIndex, localCompleted)

        // Do not refresh series TTL on single-chapter pulls.
        val merged = SourceProgressPolicy.merge(
            localPageIndex = localPageIndex,
            localCompleted = localCompleted,
            remote = outcome.remote,
            localLastModified = chapter.lastModifiedAt.toEpochMilliseconds(),
            initialSync = !hasCompletedInitialSync(mangaId),
        )
        if (merged.acceptRemote) {
            outcome.remote?.let { noteRemoteProgress(mangaId, chapter.url, it.lastModified) }
            updateChapter.await(
                ChapterUpdate(chapter.id) {
                    lastPageRead = merged.pageIndex
                    read = merged.completed
                },
            )
        }
        if (merged.schedulePush) {
            enqueue(
                mangaId,
                sourceId,
                ChapterProgress(
                    chapterUrl = chapter.url,
                    pageIndex = localPageIndex,
                    completed = localCompleted,
                    lastModified = System.currentTimeMillis(),
                ),
            )
            flushAsync(source, manga, mangaId)
        }
        return ReaderPullResult(merged.pageIndex, merged.completed)
    }

    suspend fun pullForSeriesOpen(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapters: List<Chapter>,
        sourceId: Long,
        force: Boolean = false,
    ) {
        ensureLoaded()
        if (chapters.isEmpty()) return
        if (!force && isFresh(mangaId)) return

        val targets = chapters.filter { !hasPending(mangaId, it.url) }
        if (targets.isEmpty()) {
            markProgressPullSuccess(mangaId)
            return
        }

        val remoteList = withTimeoutOrNull(seriesTimeoutMs) {
            try {
                source.getProgress(manga, targets.map { it.url })
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logcat(LogPriority.WARN) {
                    "$LOG_TAG getProgress series manga=$mangaId: ${e.message}"
                }
                null
            }
        } ?: return

        markProgressPullSuccess(mangaId)
        val remoteByUrl = remoteList.associateBy { it.chapterUrl }
        val toPush = mutableListOf<ChapterProgress>()
        val updates = targets.mapNotNull { chapter ->
            val remote = remoteByUrl[chapter.url]
            // Re-check after the network call: reader may have enqueued while we waited.
            if (hasPending(mangaId, chapter.url)) {
                return@mapNotNull null
            }
            val merged = SourceProgressPolicy.merge(
                localPageIndex = chapter.lastPageRead,
                localCompleted = chapter.read,
                remote = remote,
                localLastModified = chapter.lastModifiedAt.toEpochMilliseconds(),
                initialSync = !hasCompletedInitialSync(mangaId),
            )
            if (!merged.acceptRemote) {
                if (merged.schedulePush) {
                    toPush += ChapterProgress(
                        chapterUrl = chapter.url,
                        pageIndex = chapter.lastPageRead,
                        completed = chapter.read,
                        lastModified = chapter.lastModifiedAt.toEpochMilliseconds()
                            .takeIf { it > 0L } ?: System.currentTimeMillis(),
                    )
                }
                return@mapNotNull null
            }
            dropQueued(mangaId, chapter.url)
            remote?.let { noteRemoteProgress(mangaId, chapter.url, it.lastModified) }
            ChapterUpdate(chapter.id) {
                lastPageRead = merged.pageIndex
                read = merged.completed
            }
        }
        if (toPush.isNotEmpty()) {
            enqueueAll(mangaId, sourceId, toPush)
        }
        if (updates.isNotEmpty()) {
            logcat(LogPriority.DEBUG) {
                "$LOG_TAG series pull applied ${updates.size} manga=$mangaId"
            }
            updateChapter.awaitAll(updates)
        }
        markInitialSyncDone(mangaId)
    }

    suspend fun syncSeries(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapters: List<Chapter>,
        sourceId: Long,
        force: Boolean = true,
    ) {
        ensureLoaded()
        flush(source, manga, mangaId, respectBackoff = true)
        pullForSeriesOpen(source, manga, mangaId, chapters, sourceId, force = force)
        flush(source, manga, mangaId, respectBackoff = true)
    }

    fun flushAsync(source: ProgressAwareSource, manga: SManga, mangaId: Long) {
        scope.launch {
            try {
                flush(source, manga, mangaId, respectBackoff = true)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logcat(LogPriority.WARN) { "$LOG_TAG flushAsync manga=$mangaId: ${e.message}" }
            }
        }
    }

    /**
     * @param respectBackoff when false (worker after delay), always attempt the push.
     * @return true if the manga queue is empty after this call.
     */
    suspend fun flush(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        respectBackoff: Boolean = true,
    ): Boolean {
        ensureLoaded()
        if (respectBackoff) {
            val until = backoffUntil[mangaId] ?: 0L
            if (System.currentTimeMillis() < until) return !hasPendingQueue(mangaId)
        }

        flushDirty.add(mangaId)
        if (!flushInFlight.add(mangaId)) {
            return !hasPendingQueue(mangaId)
        }
        try {
            while (true) {
                flushDirty.remove(mangaId)
                val batch = queueMutex.withLock {
                    persistQueue()
                    expireStale(mangaId)
                    pendingPush[mangaId]?.entries?.map { it.key to it.value }.orEmpty()
                }
                if (batch.isEmpty()) {
                    if (flushDirty.contains(mangaId)) continue
                    break
                }

                val progressBatch = batch.map { (url, stored) -> stored.toChapterProgress(url) }
                val requestedUrls = progressBatch.map { it.chapterUrl }.toSet()
                try {
                    withContext(NonCancellable) {
                        val resolved = withTimeoutOrNull(flushTimeoutMs) {
                            source.updateProgress(manga, progressBatch)
                        }
                        if (resolved == null) {
                            logcat(LogPriority.WARN) { "$LOG_TAG flush timeout manga=$mangaId" }
                            bumpBackoff(mangaId)
                            scheduleRetryWork(mangaId)
                            return@withContext
                        }
                        val resolvedSet = resolved.filter { it in requestedUrls }.toSet()
                        if (progressBatch.isNotEmpty() && resolvedSet.isEmpty()) {
                            logcat(LogPriority.WARN) { "$LOG_TAG flush resolved none manga=$mangaId" }
                            bumpBackoff(mangaId)
                            scheduleRetryWork(mangaId)
                            return@withContext
                        }
                        queueMutex.withLock {
                            val map = pendingPush[mangaId] ?: return@withLock
                            for (url in resolvedSet) {
                                val snapshot = batch.find { it.first == url }?.second ?: continue
                                val cur = map[url] ?: continue
                                if (cur.revision == snapshot.revision) {
                                    map.remove(url)
                                }
                            }
                            // Unresolved entries: count an attempt so partial success cannot
                            // keep a poison pill alive indefinitely via clearBackoff.
                            for ((url, stored) in map.toList()) {
                                if (url !in resolvedSet) {
                                    map[url] = stored.copy(attempts = stored.attempts + 1)
                                }
                            }
                            if (map.isEmpty()) {
                                pendingPush.remove(mangaId)
                                pendingSourceId.remove(mangaId)
                            }
                            persistQueue()
                        }
                        if (hasPendingQueue(mangaId)) {
                            bumpBackoff(mangaId)
                            scheduleRetryWork(mangaId)
                        } else {
                            clearBackoff(mangaId)
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logcat(LogPriority.WARN) { "$LOG_TAG flush manga=$mangaId: ${e.message}" }
                    bumpBackoff(mangaId)
                    scheduleRetryWork(mangaId)
                }

                if (flushDirty.contains(mangaId)) continue
                break
            }
        } finally {
            flushInFlight.remove(mangaId)
            if (flushDirty.contains(mangaId)) {
                // Wake-up lost while we held in-flight: run again.
                flush(source, manga, mangaId, respectBackoff = respectBackoff)
            }
        }
        return !hasPendingQueue(mangaId)
    }

    suspend fun retryAllPending(): Boolean {
        ensureLoaded()
        var allClear = true
        val mangaIds = pendingPush.keys.toList()
        for (mangaId in mangaIds) {
            val sourceId = pendingSourceId[mangaId] ?: continue
            val manga = getManga.await(mangaId) ?: continue
            val source = sourceManager.get(sourceId) as? ProgressAwareSource ?: continue
            val clear = flush(source, manga.toSManga(), mangaId, respectBackoff = false)
            if (!clear) allClear = false
        }
        return allClear && !hasPendingQueue()
    }

    private suspend fun expireStale(mangaId: Long) {
        val map = pendingPush[mangaId] ?: return
        val now = System.currentTimeMillis()
        val iter = map.entries.iterator()
        while (iter.hasNext()) {
            val e = iter.next()
            if (now - e.value.firstQueuedAt > ENTRY_MAX_AGE_MS || e.value.attempts >= ENTRY_MAX_ATTEMPTS) {
                iter.remove()
            }
        }
        if (map.isEmpty()) {
            pendingPush.remove(mangaId)
            pendingSourceId.remove(mangaId)
        }
    }

    private suspend fun bumpBackoff(mangaId: Long) {
        val attempt = (backoffAttempt[mangaId] ?: 0) + 1
        backoffAttempt[mangaId] = attempt
        val delay = min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * (1L shl (attempt - 1).coerceAtMost(10)))
        backoffUntil[mangaId] = System.currentTimeMillis() + delay
        queueMutex.withLock {
            val map = pendingPush[mangaId] ?: return@withLock
            for ((url, stored) in map.toList()) {
                map.replace(url, stored, stored.copy(attempts = stored.attempts + 1))
            }
            persistQueue()
        }
    }

    private fun clearBackoff(mangaId: Long) {
        backoffUntil.remove(mangaId)
        backoffAttempt.remove(mangaId)
    }

    /** Delay until this manga's backoff window ends, if any. */
    fun backoffDelayMs(mangaId: Long = -1L): Long {
        if (mangaId >= 0) {
            val until = backoffUntil[mangaId] ?: return 0L
            return (until - System.currentTimeMillis()).coerceAtLeast(0L)
        }
        val now = System.currentTimeMillis()
        return backoffUntil.values.maxOfOrNull { (it - now).coerceAtLeast(0L) } ?: 0L
    }

    private fun scheduleRetryWork(mangaId: Long = -1L) {
        try {
            val delayMs = backoffDelayMs(mangaId).coerceAtLeast(5_000L)
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()
            val request = OneTimeWorkRequestBuilder<SourceProgressFlushWorker>()
                .setConstraints(constraints)
                .setInitialDelay(delayMs, TimeUnit.MILLISECONDS)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .build()
            context.workManager.enqueueUniqueWork(
                WORK_NAME,
                ExistingWorkPolicy.KEEP,
                request,
            )
        } catch (e: IllegalStateException) {
            logcat(LogPriority.DEBUG) { "$LOG_TAG WorkManager unavailable: ${e.message}" }
        }
    }

    private suspend fun dropQueued(mangaId: Long, chapterUrl: String) {
        queueMutex.withLock {
            val map = pendingPush[mangaId] ?: return@withLock
            if (map.remove(chapterUrl) != null) {
                if (map.isEmpty()) {
                    pendingPush.remove(mangaId)
                    pendingSourceId.remove(mangaId)
                }
                persistQueue()
            }
        }
    }

    @Serializable
    private data class StoredProgress(
        val pageIndex: Int,
        val completed: Boolean = false,
        val lastModified: Long = 0L,
        val revision: Long = 0L,
        val firstQueuedAt: Long = 0L,
        val attempts: Int = 0,
    ) {
        fun toChapterProgress(url: String) = ChapterProgress(
            chapterUrl = url,
            pageIndex = pageIndex,
            completed = completed,
            lastModified = lastModified,
        )

        companion object {
            fun from(p: ChapterProgress, existing: StoredProgress?, now: Long) = StoredProgress(
                pageIndex = p.pageIndex,
                completed = p.completed,
                lastModified = if (p.lastModified > 0L) p.lastModified else now,
                revision = (existing?.revision ?: 0L) + 1L,
                firstQueuedAt = existing?.firstQueuedAt ?: now,
                attempts = existing?.attempts ?: 0,
            )
        }
    }

    companion object {
        private const val QUEUE_PREF_KEY = "source_progress_offline_queue"
        private const val SOURCE_ID_PREF_KEY = "source_progress_source_ids"
        private const val FLUSH_TIMEOUT_MS = 60_000L
        private const val SERIES_TIMEOUT_MS = 60_000L
        private const val READER_TIMEOUT_MS = 1_500L
        private const val ENTRY_MAX_AGE_MS = 7L * 24 * 60 * 60 * 1000
        private const val ENTRY_MAX_ATTEMPTS = 20
        private const val BACKOFF_BASE_MS = 30_000L
        private const val BACKOFF_MAX_MS = 6 * 60 * 60 * 1000L
        private const val WORK_NAME = "SourceProgressFlush"

        const val LOG_TAG = "SourceProgress"
    }
}
