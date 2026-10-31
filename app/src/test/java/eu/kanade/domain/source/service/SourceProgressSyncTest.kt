package eu.kanade.domain.source.service

import eu.kanade.tachiyomi.source.ProgressAwareSource
import eu.kanade.tachiyomi.source.model.ChapterProgress
import eu.kanade.tachiyomi.source.model.SManga
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore
import tachiyomi.domain.chapter.interactor.UpdateChapter
import tachiyomi.domain.chapter.model.Chapter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Coordinator tests for offline queue, flush timeout retention, dirty re-run,
 * skip-pull when pending, and concurrent enqueue during push.
 */
class SourceProgressSyncTest {

    private val updateChapter = mockk<UpdateChapter>(relaxed = true)

    private fun store(): MutablePreferenceStore = MutablePreferenceStore()

    private fun sync(store: PreferenceStore = store()): SourceProgressSync =
        SourceProgressSync(updateChapter, store, mockk(relaxed = true), mockk(relaxed = true), mockk(relaxed = true), mockk(relaxed = true))

    private fun manga(): SManga = SManga.create().apply {
        url = "https://example.com/series/1"
        title = "Test"
    }

    private fun chapter(
        url: String = "https://example.com/ch/1",
        lastPage: Int = 0,
        read: Boolean = false,
    ) = Chapter.create().copy(
        id = 1L,
        mangaId = 10L,
        url = url,
        name = "Ch 1",
        lastPageRead = lastPage,
        read = read,
    )

    private fun progress(
        url: String = "https://example.com/ch/1",
        page: Int = 3,
        completed: Boolean = false,
    ) = ChapterProgress(
        chapterUrl = url,
        pageIndex = page,
        completed = completed,
    )

    @Test
    fun `flush timeout keeps queue entries`() = runBlocking {
        val store = store()
        val coordinator = sync(store)
        coordinator.flushTimeoutMs = 50L
        val mangaId = 10L
        val m = manga()
        coordinator.enqueue(mangaId, 1L, progress(page = 5

        val source = object : ProgressAwareSource {
            override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                delay(5_000) // longer than flushTimeoutMs
            }
        
                override suspend fun getProgress(manga: SManga, chapterUrls: List<String>) =
                    emptyList<ChapterProgress>()
            }
        coordinator.flush(source, m, mangaId, respectBackoff = false)

        // Timeout must leave the queue intact for a new coordinator instance.
        val restored = sync(store)
        val page = restored.pullForReaderOpen(
            source = object : ProgressAwareSource {
                override suspend fun getProgress(
                    manga: SManga,
                    chapterUrls: List<String>,
                ): List<ChapterProgress> {
                    error("pull must be skipped while pending")
                }
            },
            manga = m,
            mangaId = mangaId,
            chapter = chapter(),
            localPageIndex = 5,
            localCompleted = false,
        )
        assertEquals(5, page)
    }

    @Test
    fun `new coordinator loads persisted queue`() = runBlocking {
        val store = store()
        val first = sync(store)
        first.enqueue(10L, 1L, progress(page = 7, completed = true

        // New instance must reload the offline queue from the preference store.
        // Flush first (do not pull — pullForReaderOpen would flushAsync and race).
        val second = sync(store)
        val flushed = AtomicReference<List<ChapterProgress>?>(null)
        second.flush(
            object : ProgressAwareSource {
                override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                    flushed.set(progress)
                }
            
                override suspend fun getProgress(manga: SManga, chapterUrls: List<String>) =
                    emptyList<ChapterProgress>()
            },
            manga(),
            10L,
        )
        val batch = flushed.get()
        assertEquals(1, batch?.size)
        assertEquals(7, batch!![0].pageIndex)
        assertTrue(batch[0].completed)
        assertEquals("https://example.com/ch/1", batch[0].chapterUrl)
    }

    @Test
    fun `overlapping flushes trigger dirty re-run`() = runBlocking {
        val store = store()
        val coordinator = sync(store)
        val mangaId = 10L
        val m = manga()
        val calls = AtomicInteger(0)
        val batches = mutableListOf<List<ChapterProgress>>()
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()

        val source = object : ProgressAwareSource {
            override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                val n = calls.incrementAndGet()
                synchronized(batches) { batches.add(progress.toList()) }
                if (n == 1) {
                    firstEntered.complete(Unit)
                    releaseFirst.await()
                }
            }
        
                override suspend fun getProgress(manga: SManga, chapterUrls: List<String>) =
                    emptyList<ChapterProgress>()
            }

        coordinator.enqueue(mangaId, 1L, progress(page = 1
        val first = async { coordinator.flush(source, m, mangaId, respectBackoff = false) }
        firstEntered.await()

        coordinator.enqueue(mangaId, 1L, progress(page = 2
        val second = async { coordinator.flush(source, m, mangaId, respectBackoff = false) }

        releaseFirst.complete(Unit)
        withTimeout(5_000) {
            first.await()
            second.await()
        }
        assertEquals(2, calls.get(), "expected exactly one dirty re-run")
        assertEquals(2, batches.size)
        assertTrue(
            batches[1].any { it.pageIndex == 2 },
            "second batch should contain page 2, got ${batches[1]}",
        )
    }

    @Test
    fun `pending chapter skips pull`() = runBlocking {
        val coordinator = sync()
        val mangaId = 10L
        coordinator.enqueue(mangaId, 1L, progress(page = 4

        var pullCalled = false
        val source = object : ProgressAwareSource {
            override suspend fun getProgress(manga: SManga, chapterUrls: List<String>): List<ChapterProgress> {
                pullCalled = true
                return listOf(progress(page = 99
            }

            override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                // flushAsync from pull path may call this
            }
        }
        val page = coordinator.pullForReaderOpen(
            source = source,
            manga = manga(),
            mangaId = mangaId,
            chapter = chapter(),
            localPageIndex = 4,
            localCompleted = false,
        )
        assertEquals(4, page)
        assertFalse(pullCalled, "getProgress must not be called when pending")
    }

    @Test
    fun `dirty re-run pushes entry enqueued during in-flight flush`() = runBlocking {
        val coordinator = sync()
        val mangaId = 10L
        val m = manga()
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val calls = AtomicInteger(0)
        val lastBatch = AtomicReference<List<ChapterProgress>?>(null)

        val source = object : ProgressAwareSource {
            override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                val n = calls.incrementAndGet()
                lastBatch.set(progress.toList())
                if (n == 1) {
                    firstEntered.complete(Unit)
                    releaseFirst.await()
                }
            }
        
                override suspend fun getProgress(manga: SManga, chapterUrls: List<String>) =
                    emptyList<ChapterProgress>()
            }

        coordinator.enqueue(mangaId, 1L, progress(page = 1
        val flushJob = async { coordinator.flush(source, m, mangaId, respectBackoff = false) }
        firstEntered.await()

        // Enqueue a newer record while the first push is in flight.
        coordinator.enqueue(mangaId, 1L, progress(page = 9
        // Mark dirty so the running flush re-runs after success.
        val overlapping = async { coordinator.flush(source, m, mangaId, respectBackoff = false) }

        releaseFirst.complete(Unit)
        withTimeout(5_000) {
            flushJob.await()
            overlapping.await()
        }

        // After both complete, queue should be empty (second push succeeded with page 9).
        // A subsequent flush with a counting source must see an empty batch.
        var thirdCalls = 0
        coordinator.flush(
            object : ProgressAwareSource {
                override suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String> {
                    thirdCalls++
                    error("queue should be empty, got $progress", respectBackoff = false)
                }
            
                override suspend fun getProgress(manga: SManga, chapterUrls: List<String>) =
                    emptyList<ChapterProgress>()
            },
            m,
            mangaId,
        )
        assertEquals(0, thirdCalls, "no third push when queue empty")
        assertEquals(2, calls.get())
        assertTrue(lastBatch.get()!!.any { it.pageIndex == 9 })
    }

    /**
     * PreferenceStore that shares mutable values across getString() calls,
     * so a second SourceProgressSync instance can load the offline queue.
     */
    private class MutablePreferenceStore : PreferenceStore {
        private val data = ConcurrentHashMap<String, Any?>()

        private inner class Pref<T>(
            private val key: String,
            private val defaultValue: T,
        ) : Preference<T> {
            override fun key() = key
            @Suppress("UNCHECKED_CAST")
            override fun get(): T = (data[key] as? T) ?: defaultValue
            override fun set(value: T) {
                data[key] = value
            }
            override fun isSet() = data.containsKey(key)
            override fun delete() {
                data.remove(key)
            }
            override fun defaultValue() = defaultValue
            override fun changes() = kotlinx.coroutines.flow.flow { emit(get()) }
            override fun stateIn(scope: kotlinx.coroutines.CoroutineScope): StateFlow<T> =
                MutableStateFlow(get())
        }

        override fun getString(key: String, defaultValue: String): Preference<String> =
            Pref(key, defaultValue)
        override fun getLong(key: String, defaultValue: Long): Preference<Long> =
            Pref(key, defaultValue)
        override fun getInt(key: String, defaultValue: Int): Preference<Int> =
            Pref(key, defaultValue)
        override fun getFloat(key: String, defaultValue: Float): Preference<Float> =
            Pref(key, defaultValue)
        override fun getBoolean(key: String, defaultValue: Boolean): Preference<Boolean> =
            Pref(key, defaultValue)
        override fun getStringSet(key: String, defaultValue: Set<String>): Preference<Set<String>> =
            Pref(key, defaultValue)
        override fun <T> getObjectFromString(
            key: String,
            defaultValue: T,
            serializer: (T) -> String,
            deserializer: (String) -> T,
        ): Preference<T> = Pref(key, defaultValue)
        override fun <T> getObjectFromInt(
            key: String,
            defaultValue: T,
            serializer: (T) -> Int,
            deserializer: (Int) -> T,
        ): Preference<T> = Pref(key, defaultValue)
        override fun <T> getObjectSetFromStringSet(
            key: String,
            defaultValue: Set<T>,
            serializer: (T) -> String,
            deserializer: (String) -> T?,
        ): Preference<Set<T>> = Pref(key, defaultValue)
        override fun getAll(): Map<String, *> = data.toMap()
    }
}
