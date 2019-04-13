package database.layer_1_log

import database.layer_0_file.MemoryFileManager
import database.layer_0_file.Page
import database.types.FileName
import database.utils.isZeroed
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertFalse

internal class LogManagerTest {

    private val fm = MemoryFileManager()
    private val logManager = LogManager(fm, FileName("logfile"))
    private val buffer = ByteBuffer.allocate(Page.BLOCK_SIZE)

    @Test
    fun `insert and iterate records`() {
        logManager.append(4, "foo")
        logManager.append(3, "bar")

        val entries = logManager.toList()
        assertEquals(2, entries.size)

        assertEquals(3, entries[0].nextInt())
        assertEquals("bar", entries[0].nextString())

        assertEquals(4, entries[1].nextInt())
        assertEquals("foo", entries[1].nextString())
    }

    @Test
    fun `flush records`() {
        val lsn = logManager.append(4, "foo")

        fm.read(database.layer_0_file.Block(FileName("logfile"), 0), buffer)
        assertTrue(buffer.isZeroed)

        logManager.flush(lsn)

        fm.read(database.layer_0_file.Block(FileName("logfile"), 0), buffer)
        assertFalse(buffer.isZeroed)
    }
}
