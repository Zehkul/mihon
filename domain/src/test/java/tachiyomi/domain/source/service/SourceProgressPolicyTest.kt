package tachiyomi.domain.source.service

import eu.kanade.tachiyomi.source.model.ChapterProgress
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

@Execution(ExecutionMode.CONCURRENT)
class SourceProgressPolicyTest {

    private fun remote(page: Int, completed: Boolean = false, lastModified: Long = 0L) = ChapterProgress(
        chapterUrl = "https://example.com/ch/1",
        pageIndex = page,
        completed = completed,
        lastModified = lastModified,
    )

    @Test
    fun `null remote keeps local and schedules push when local has progress`() {
        val r = SourceProgressPolicy.merge(5, false, null)
        r.pageIndex shouldBe 5
        r.acceptRemote shouldBe false
        r.schedulePush shouldBe true
    }

    @Test
    fun `null remote with empty local does not schedule push`() {
        val r = SourceProgressPolicy.merge(0, false, null)
        r.schedulePush shouldBe false
        r.acceptRemote shouldBe false
    }

    @Test
    fun `after first sync remote unread resets local completed`() {
        val r = SourceProgressPolicy.merge(9, true, remote(0, completed = false), initialSync = false, )
        r.completed shouldBe false
        r.pageIndex shouldBe 0
        r.acceptRemote shouldBe true
    }

    @Test
    fun `after first sync remote behind is still accepted`() {
        val r = SourceProgressPolicy.merge(10, false, remote(3), initialSync = false, )
        r.pageIndex shouldBe 3
        r.acceptRemote shouldBe true
        r.schedulePush shouldBe false
    }

    @Test
    fun `first sync does not let remote unread wipe local completed`() {
        val r = SourceProgressPolicy.merge(9, true, remote(0, completed = false), initialSync = true)
        r.completed shouldBe true
        r.pageIndex shouldBe 9
        r.acceptRemote shouldBe false
        r.schedulePush shouldBe true
    }

    @Test
    fun `first sync accepts remote completed over local unread`() {
        val r = SourceProgressPolicy.merge(0, false, remote(2, completed = true), initialSync = true)
        r.completed shouldBe true
        r.acceptRemote shouldBe true
    }

    @Test
    fun `first sync accepts higher remote page`() {
        val r = SourceProgressPolicy.merge(2, false, remote(8), initialSync = true)
        r.pageIndex shouldBe 8
        r.acceptRemote shouldBe true
    }

    @Test
    fun `first sync keeps local when remote page is behind`() {
        val r = SourceProgressPolicy.merge(8, false, remote(2), initialSync = true)
        r.pageIndex shouldBe 8
        r.acceptRemote shouldBe false
        r.schedulePush shouldBe true
    }
}

    @Test
    fun `equal states do not schedule push`() {
        val r = SourceProgressPolicy.merge(5, false, remote(5), initialSync = false, )
        assertFalse(r.acceptRemote)
        assertFalse(r.schedulePush)
    }

    @Test
    fun `equal completed does not schedule push`() {
        val r = SourceProgressPolicy.merge(9, true, remote(0, completed = true), initialSync = false, )
        assertFalse(r.acceptRemote)
        assertFalse(r.schedulePush)
    }

    @Test
    fun `newer remote clock wins including reset`() {
        val r = SourceProgressPolicy.merge(
            localPageIndex = 9,
            localCompleted = true,
            remote = remote(0, completed = false, lastModified = 2000L),
            localLastModified = 1000L,
        )
        assertTrue(r.acceptRemote)
        assertFalse(r.completed)
    }

    @Test
    fun `older remote clock does not overwrite local`() {
        val r = SourceProgressPolicy.merge(
            localPageIndex = 9,
            localCompleted = true,
            remote = remote(0, completed = false, lastModified = 500L),
            localLastModified = 1000L,
        )
        assertFalse(r.acceptRemote)
        assertTrue(r.schedulePush)
    }
