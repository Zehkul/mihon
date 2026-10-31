package eu.kanade.tachiyomi.source.model

/**
 * Page-level reading progress for one chapter, owned by the source.
 *
 * [pageIndex] is 0-based and uses the same coordinate as [Page.index] from
 * getPageList. When [completed] is true, pageIndex is meaningless and ignored.
 *
 * pageIndex 0 with completed false means reset to unread. Hosts send that only
 * when local state changes into unread (explicit mark-unread, or progress
 * rewound to the start), never for a chapter the user has not touched. Sources
 * that cannot distinguish never-read from reset to unread should return
 * (0, false) for both.
 *
 * [lastModified] is epoch milliseconds for when this progress was last changed.
 * Hosts send the device time of the local change on push. Sources return the
 * server time on pull, or 0 when unknown. When both sides provide a non-zero
 * value, the newer one wins. A zero value means "no clock"; hosts then fall
 * back to page/completed comparison and will not apply a remote reset over
 * local progress.
 *
 * New fields require a new secondary constructor (keeping this constructor) or
 * a library major version so already-compiled extensions keep linking.
 */
class ChapterProgress(
    val chapterUrl: String,
    val pageIndex: Int,
    val completed: Boolean,
    val lastModified: Long = 0L,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ChapterProgress) return false
        return chapterUrl == other.chapterUrl &&
            pageIndex == other.pageIndex &&
            completed == other.completed &&
            lastModified == other.lastModified
    }

    override fun hashCode(): Int {
        var result = chapterUrl.hashCode()
        result = 31 * result + pageIndex
        result = 31 * result + completed.hashCode()
        result = 31 * result + lastModified.hashCode()
        return result
    }

    // chapterUrl omitted: may contain tokens in query strings on some sources.
    override fun toString(): String =
        "ChapterProgress(pageIndex=$pageIndex, completed=$completed, " +
            "lastModified=$lastModified)"
}
