package tachiyomi.domain.source.service

import eu.kanade.tachiyomi.source.model.ChapterProgress

/**
 * Pure merge rules for source-owned page progress.
 *
 * Host responsibility: do not call [merge] for a chapter that still has a
 * pending offline-queue entry—those mean local is newer and the pull must
 * not overwrite them.
 */
object SourceProgressPolicy {

    data class Result(
        val pageIndex: Int,
        val completed: Boolean,
        val acceptRemote: Boolean,
        val schedulePush: Boolean,
    )

    /**
     * @param localLastModified epoch ms of the local progress change, or 0 if
     *   unknown (e.g. never noted on this device).
     */
    fun merge(
        localPageIndex: Int,
        localCompleted: Boolean,
        remote: ChapterProgress?,
        localLastModified: Long = 0L,
        initialSync: Boolean = false,
    ): Result {
        if (remote == null) {
            val hasLocal = localCompleted || localPageIndex > 0
            return Result(
                pageIndex = localPageIndex,
                completed = localCompleted,
                acceptRemote = false,
                schedulePush = hasLocal,
            )
        }

        if (statesEqual(localPageIndex, localCompleted, remote)) {
            return Result(
                pageIndex = localPageIndex,
                completed = localCompleted,
                acceptRemote = false,
                schedulePush = false,
            )
        }

        val remoteClock = remote.lastModified
        val hasLocalProgress = localCompleted || localPageIndex > 0
        val explicitLocalUnread =
            localLastModified > 0L && !localCompleted && localPageIndex == 0
        val hasLocalDecision = hasLocalProgress || explicitLocalUnread
        val explicitRemoteUnread = !remote.completed && remote.pageIndex == 0

        // Both sides timestamped: newer wins (including resets either way).
        if (localLastModified > 0L && remoteClock > 0L) {
            return if (remoteClock > localLastModified) {
                Result(
                    pageIndex = remote.pageIndex,
                    completed = remote.completed,
                    acceptRemote = true,
                    schedulePush = false,
                )
            } else {
                Result(
                    pageIndex = localPageIndex,
                    completed = localCompleted,
                    acceptRemote = false,
                    schedulePush = hasLocalDecision,
                )
            }
        }

        // Local timestamp only: keep local (including mark-unread).
        if (localLastModified > 0L && remoteClock == 0L) {
            return Result(
                pageIndex = localPageIndex,
                completed = localCompleted,
                acceptRemote = false,
                schedulePush = hasLocalDecision,
            )
        }

        // Remote timestamp only: trust the server (our local clock was lost).
        if (localLastModified == 0L && remoteClock > 0L) {
            return Result(
                pageIndex = remote.pageIndex,
                completed = remote.completed,
                acceptRemote = true,
                schedulePush = false,
            )
        }

        // No clocks. Accept remote when ahead. Never push local completed over
        // remote explicit unread — that is what clobbered server-side resets.
        if (isRemoteAhead(localPageIndex, localCompleted, remote)) {
            return Result(
                pageIndex = remote.pageIndex,
                completed = remote.completed,
                acceptRemote = true,
                schedulePush = false,
            )
        }
        if (explicitRemoteUnread && localCompleted) {
            return Result(
                pageIndex = remote.pageIndex,
                completed = remote.completed,
                acceptRemote = true,
                schedulePush = false,
            )
        }
        return Result(
            pageIndex = localPageIndex,
            completed = localCompleted,
            acceptRemote = false,
            schedulePush = hasLocalDecision && !explicitRemoteUnread,
        )
    }

    private fun statesEqual(
        localPageIndex: Int,
        localCompleted: Boolean,
        remote: ChapterProgress,
    ): Boolean {
        if (localCompleted != remote.completed) return false
        if (localCompleted) return true
        return localPageIndex == remote.pageIndex
    }

    private fun isRemoteAhead(
        localPageIndex: Int,
        localCompleted: Boolean,
        remote: ChapterProgress,
    ): Boolean {
        if (remote.completed && !localCompleted) return true
        if (!remote.completed && localCompleted) return false
        return remote.pageIndex > localPageIndex
    }
}
