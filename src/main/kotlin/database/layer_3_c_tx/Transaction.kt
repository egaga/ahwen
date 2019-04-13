package database.layer_3_c_tx

import database.layer_1_log.LogManager
import database.layer_2_buffer.BufferManager
import database.layer_2_buffer.PageFormatter
import database.layer_3_a_tx.recovery.RecoveryManager
import database.layer_3_b_tx.concurrency.ConcurrencyManager
import database.layer_3_b_tx.concurrency.LockTable
import database.layer_6_query.SqlInt
import database.layer_6_query.SqlValue
import database.types.FileName
import database.types.SqlType
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
class Transaction(logManager: LogManager, bufferManager: BufferManager, lockTable: LockTable, private val fileManager: database.layer_0_file.FileManager) {

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

    fun pin(block: database.layer_0_file.Block) {
        myBuffers.pin(block)
    }

    fun unpin(block: database.layer_0_file.Block) {
        myBuffers.unpin(block)
    }

    fun getValue(block: database.layer_0_file.Block, offset: Int, type: SqlType): SqlValue {
        concurrencyManager.acquireSharedLock(block)
        val buffer = myBuffers.getBuffer(block)
        return buffer.getValue(offset, type)
    }

    fun setValue(block: database.layer_0_file.Block, offset: Int, value: SqlValue) {
        concurrencyManager.acquireExclusiveLock(block)
        val buffer = myBuffers.getBuffer(block)
        val lsn = recoveryManager.setValue(buffer, offset, value)
        buffer.setValue(offset, value, txnum, lsn)
    }

    fun size(fileName: FileName): Int {
        concurrencyManager.acquireSharedLock(eofBlock(fileName))
        return fileManager.size(fileName)
    }

    fun append(fileName: FileName, formatter: PageFormatter): database.layer_0_file.Block {
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
        private fun eofBlock(fileName: FileName) = database.layer_0_file.Block(fileName, -1)
    }
}

fun Transaction.getInt(block: database.layer_0_file.Block, offset: Int): Int =
    (getValue(block, offset, SqlType.INTEGER) as SqlInt).value

fun Transaction.setInt(block: database.layer_0_file.Block, offset: Int, value: Int) {
    setValue(block, offset, SqlInt(value))
}
