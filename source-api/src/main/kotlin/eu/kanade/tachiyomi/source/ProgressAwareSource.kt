package eu.kanade.tachiyomi.source

import eu.kanade.tachiyomi.source.model.ChapterProgress
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.model.SManga

/**
 * Optional capability for sources that store reading progress on their own
 * server (self-hosted readers and similar).
 *
 * Progress is keyed by the exact [SChapter.url] the host passed
 * ([ChapterProgress.chapterUrl]). Chapters that are slices of a larger file
 * use relative [ChapterProgress.pageIndex] values for those exact URLs.
 *
 * A pushed (0, false) means the user's local state is unread (explicitly
 * marked unread, or progress rewound to the start); sources should reset
 * their record. Hosts send it only when local state changes into it, never
 * for a chapter the user has not touched. Sources that cannot distinguish
 * never-read from reset to unread should return (0, false) for both.
 * Omitting a chapter URL from [getProgress] means the source has no record.
 *
 * Prefer non-zero [ChapterProgress.lastModified] on both push and pull so
 * hosts can resolve conflicts by newest change. Zero means the clock is
 * unknown for that record.
 *
 * Do not swallow `kotlinx.coroutines.CancellationException`: hosts cancel
 * slow calls and rely on cooperative cancellation. Partially applied pushes
 * are retried only for URLs that were not resolved; implementations must be
 * idempotent. Sources should batch network work for large lists.
 */
interface ProgressAwareSource {

    /**
     * Push progress records. Idempotent. The host sends at most one record
     * per chapter URL per call.
     *
     * Return the chapter URLs that were resolved (applied, or permanently
     * rejected such as HTTP 404 for a deleted chapter). Resolved URLs will
     * not be sent again until local progress changes. Auth and network errors
     * are never permanently rejected: throw so the host can retry. Returning
     * an empty list for a non-empty batch is treated by the host like a throw.
     * Prefer returning a partial list over throwing when some items succeed.
     */
    suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>): List<String>

    /**
     * Pull progress for the given chapter URLs.
     *
     * All-or-nothing: throw if any requested chapter cannot be answered, or
     * return a record for every URL that has a known record. Omit a URL only
     * when the source positively knows there is no record.
     */
    suspend fun getProgress(manga: SManga, chapterUrls: List<String>): List<ChapterProgress>
}
