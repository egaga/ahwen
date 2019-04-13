package database.layer_0_file

import database.types.FileName
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A simple in-memory implementation of [FileManager] useful for unit tests.
 */
class MemoryFileManager : database.layer_0_file.FileManager {

    private val blocksByFiles = mutableMapOf<FileName, Int>()
    private val dataByBlock = mutableMapOf<database.layer_0_file.Block, ByteArray>()
    private val lock = ReentrantLock()

    override fun read(block: database.layer_0_file.Block, bb: ByteBuffer) {
        lock.withLock {
            bb.position(0)
            bb.put(getBlockData(block))
        }
    }

    override fun write(block: database.layer_0_file.Block, bb: ByteBuffer) {
        lock.withLock {
            bb.position(0)
            bb.get(getBlockData(block))
        }
    }

    override fun append(fileName: FileName, bb: ByteBuffer): database.layer_0_file.Block {
        lock.withLock {
            val size = size(fileName)
            blocksByFiles[fileName] = size + 1
            val block = database.layer_0_file.Block(fileName, size)
            write(block, bb)
            return block
        }
    }

    override fun size(fileName: FileName): Int =
        lock.withLock {
            blocksByFiles.getOrPut(fileName) { 0 }
        }

    private fun getBlockData(block: database.layer_0_file.Block): ByteArray =
        dataByBlock.getOrPut(block) { ByteArray(Page.BLOCK_SIZE) }
}
