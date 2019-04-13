package dev.komu.ahwen.layer_3_c_tx

import dev.komu.ahwen.layer_2_buffer.BufferManager
import dev.komu.ahwen.layer_2_buffer.PageFormatter
import dev.komu.ahwen.layer_0_file.Block
import dev.komu.ahwen.layer_0_file.FileManager
import dev.komu.ahwen.layer_1_log.LogManager
import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.SqlInt
import dev.komu.ahwen.layer_3_b_tx.concurrency.ConcurrencyManager
import dev.komu.ahwen.layer_3_b_tx.concurrency.LockTable
import dev.komu.ahwen.layer_3_a_tx.recovery.RecoveryManager
import dev.komu.ahwen.types.FileName
import dev.komu.ahwen.types.SqlType
import java.util.concurrent.atomic.AtomicInteger

/**
 * A facade for managing transactional operations. Integrates with [RecoveryManager]
 * and [ConcurrencyManager] to provide isolation and durability guarantees for data.
 *
 * Currently implements only serializable transaction level. This is because all locks
 * are held to the end of transaction and the exclusive lock taken on [append] will
 * prevent [size] from taking the file size if new blocks are appended.
 *
 * Less strict isolation levels could be achieved by:
 *
 * - repeatable read: don't acquire lock on [size]
 * - read committed: don't acquire lock on [size], release shared locks before commit
 * - read uncommitted: never acquire shared locks
 */
class Transaction(logManager: LogManager, bufferManager: BufferManager, lockTable: LockTable, private val fileManager: FileManager) {

    private val txnum = TransactionNumber(nextTxNum.getAndDecrement())
    private val recoveryManager = RecoveryManager(txnum, logManager, bufferManager)
    private val concurrencyManager = ConcurrencyManager(lockTable)
    private val myBuffers = BufferList(bufferManager)

    fun commit() {
        myBuffers.unpinAll()
        recoveryManager.commit()
        concurrencyManager.releaseAllLocks()
    }

    fun rollback() {
        myBuffers.unpinAll()
        recoveryManager.rollback()
        concurrencyManager.releaseAllLocks()
    }

    fun recover() {
        recoveryManager.recover()
    }

    fun pin(block: Block) {
        myBuffers.pin(block)
    }

    fun unpin(block: Block) {
        myBuffers.unpin(block)
    }

    fun getValue(block: Block, offset: Int, type: SqlType): SqlValue {
        concurrencyManager.acquireSharedLock(block)
        val buffer = myBuffers.getBuffer(block)
        return buffer.getValue(offset, type)
    }

    fun setValue(block: Block, offset: Int, value: SqlValue) {
        concurrencyManager.acquireExclusiveLock(block)
        val buffer = myBuffers.getBuffer(block)
        val lsn = recoveryManager.setValue(buffer, offset, value)
        buffer.setValue(offset, value, txnum, lsn)
    }

    fun size(fileName: FileName): Int {
        concurrencyManager.acquireSharedLock(eofBlock(fileName))
        return fileManager.size(fileName)
    }

    fun append(fileName: FileName, formatter: PageFormatter): Block {
        concurrencyManager.acquireExclusiveLock(eofBlock(fileName))
        val block = myBuffers.pinNew(fileName, formatter)
        unpin(block)
        return block
    }

    companion object {
        private val nextTxNum = AtomicInteger(0)

        /**
         * Returns a dummy block representing the end-of-file for given file. The block can't be
         * read since it has an invalid number, but it can be locked to achieve serializable isolation.
         */
        private fun eofBlock(fileName: FileName) = Block(fileName, -1)
    }
}

fun Transaction.getInt(block: Block, offset: Int): Int =
    (getValue(block, offset, SqlType.INTEGER) as SqlInt).value

fun Transaction.setInt(block: Block, offset: Int, value: Int) {
    setValue(block, offset, SqlInt(value))
}