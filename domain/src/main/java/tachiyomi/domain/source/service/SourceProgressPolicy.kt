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
     * @param localLastModified epoch ms of the local change, or 0 if unknown.
     * @param initialSync true until a series has completed one successful pull.
     *   During initial sync, remote is accepted only when ahead or when a
     *   non-zero remote clock is strictly newer than local.
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
        val bothHaveClocks = localLastModified > 0L && remoteClock > 0L
        if (bothHaveClocks) {
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
                    schedulePush = localCompleted || localPageIndex > 0,
                )
            }
        }

        // Missing clocks: accept remote only when ahead; never apply a remote
        // reset over local progress. During initial sync the same rule applies
        // so server never-read records cannot wipe local history.
        return if (isRemoteAhead(localPageIndex, localCompleted, remote)) {
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
                schedulePush = localCompleted || localPageIndex > 0,
            )
        }
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
