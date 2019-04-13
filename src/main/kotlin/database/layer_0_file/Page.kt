@file:Suppress("UsePropertyAccessSyntax")

package database.layer_0_file

import database.layer_0_file.Page.Companion.BLOCK_SIZE
import database.layer_6_query.SqlValue
import database.layer_6_query.SqlInt
import database.layer_6_query.SqlString
import database.types.FileName
import database.types.SqlType
import database.types.SqlType.*
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A page is [BLOCK_SIZE] worth of raw data loaded to memory.
 *
 * Pages will get reused: a single page will typically contain data from multiple [Block]s
 * during its lifetime.
 */
class Page(private val fileManager: database.layer_0_file.FileManager) {

    private val contents: ByteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE)
    private val lock = ReentrantLock()

    fun read(block: database.layer_0_file.Block) {
        lock.withLock {
            fileManager.read(block, contents)
        }
    }

    fun write(block: database.layer_0_file.Block) {
        lock.withLock {
            fileManager.write(block, contents)
        }
    }

    fun append(fileName: FileName): database.layer_0_file.Block {
        lock.withLock {
            return fileManager.append(fileName, contents)
        }
    }

    operator fun set(offset: Int, value: SqlValue) {
        lock.withLock {
            contents.position(offset)
            when (value) {
                is SqlInt ->
                    contents.putInt(value.value)
                is SqlString ->
                    contents.writeString(value.value)
            }
        }
    }

    fun getValue(offset: Int, type: SqlType): SqlValue =
        lock.withLock {
            contents.position(offset)
            when (type) {
                INTEGER -> SqlInt(contents.getInt())
                VARCHAR -> SqlString(contents.readString())
            }
        }

    companion object {

        /**
         * Size of blocks, ie. the size of individual I/O operations. For best performance
         * this should correspond to the device block size of the operating system (typically 4k),
         * but it might be interesting to make it lower for testing purposes.
         */
        const val BLOCK_SIZE = 4096

        const val INT_SIZE = Int.SIZE_BYTES

        private fun ByteBuffer.readString(): String {
            val len = getInt()
            val bytes = ByteArray(len)

            get(bytes)
            return String(bytes, SqlString.charset)
        }

        private fun ByteBuffer.writeString(value: String) {
            val bytes = value.toByteArray(SqlString.charset)

            putInt(bytes.size)
            put(bytes)
        }
    }
}

fun Page.getInt(offset: Int): Int =
    (getValue(offset, SqlType.INTEGER) as SqlInt).value

operator fun Page.set(offset: Int, value: Int) {
    this[offset] = SqlInt(value)
}
