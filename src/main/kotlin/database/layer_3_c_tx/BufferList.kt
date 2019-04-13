package database.layer_3_c_tx

import database.layer_2_buffer.Buffer
import database.layer_2_buffer.BufferManager
import database.layer_2_buffer.PageFormatter
import database.types.FileName

/**
 * List of [Buffer]s pinned by a transaction.
 */
class BufferList(private val bufferManager: BufferManager) {

    private val buffers = mutableMapOf<database.layer_0_file.Block, Buffer>()
    private val pins = arrayListOf<database.layer_0_file.Block>()

    /**
     * Returns buffer for a block that has been [pin]ned earlier.
     *
     * @throws IllegalStateException if the block is not pinned by this transaction
     */
    fun getBuffer(block: database.layer_0_file.Block): Buffer =
        buffers[block] ?: error("no buffer for block $block")

    /**
     * Pin given block.
     */
    fun pin(block: database.layer_0_file.Block) {
        val buffer = bufferManager.pin(block)
        buffers[block] = buffer
        pins += block
    }

    /**
     * Create a new block on given [fileName], format it using [formatter] and pin it.
     */
    fun pinNew(fileName: FileName, formatter: PageFormatter): database.layer_0_file.Block {
        val buffer = bufferManager.pinNew(fileName, formatter)
        val block = buffer.block ?: error("no block for new buffer")
        buffers[block] = buffer
        pins += block
        return block
    }

    /**
     * Unpin a previously pinned block.
     *
     * @throws IllegalStateException if the block is not pinned by this transaction
     */
    fun unpin(block: database.layer_0_file.Block) {
        val buffer = getBuffer(block)
        bufferManager.unpin(buffer)
        pins -= block
        if (block !in pins)
            buffers.remove(block)
    }

    /**
     * Unpin all blocks pinned by the transaction.
     */
    fun unpinAll() {
        for (block in pins) {
            val buffer = getBuffer(block)
            bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }
}
