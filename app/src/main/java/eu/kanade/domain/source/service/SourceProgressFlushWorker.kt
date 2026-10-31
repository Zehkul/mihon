package eu.kanade.domain.source.service

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import dev.zacsweers.metro.Inject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import logcat.LogPriority
import mihon.app.di.AppGraph
import mihon.core.metro.metroGraph
import tachiyomi.core.common.util.system.logcat

class SourceProgressFlushWorker(
    context: Context,
    params: WorkerParameters,
) : CoroutineWorker(context, params) {

    private val graph: AppGraph = context.metroGraph()

    @Inject lateinit var sourceProgressSync: SourceProgressSync

    override suspend fun doWork(): Result {
        graph.inject(this)
        if (runAttemptCount > 8) {
            return Result.failure()
        }
        return try {
            val allClear = withContext(Dispatchers.IO) {
                sourceProgressSync.retryAllPending()
            }
            if (allClear) {
                Result.success()
            } else {
                Result.retry()
            }
        } catch (e: Exception) {
            logcat(LogPriority.WARN, e) { "${SourceProgressSync.LOG_TAG} retry worker failed" }
            Result.retry()
        }
    }
}
