package database.layer_3_a_tx.recovery

import database.layer_1_log.BasicLogRecord
import database.layer_1_log.LogManager
import database.layer_1_log.LogSequenceNumber
import database.layer_2_buffer.BufferManager
import database.layer_3_c_tx.TransactionNumber
import database.layer_6_query.SqlInt
import database.layer_6_query.SqlString
import database.types.FileName

/**
 * Base class for different records of the recovery log.
 */
sealed class LogRecord {

    /**
     * Identifier for the transaction that this change belongs to or
     * `null` if the record is not logically related to any transaction.
     */
    abstract val txNumber: TransactionNumber?

    /**
     * Serializes this record to given log-manager.
     */
    abstract fun writeToLog(logManager: LogManager): LogSequenceNumber

    /**
     * Undoes the changes represented by this log-record.
     */
    open fun undo(txnum: TransactionNumber, bufferManager: BufferManager) {
    }

    companion object {

        const val CHECKPOINT = 0
        const val START = 1
        const val COMMIT = 2
        const val ROLLBACK = 3
        const val SETINT = 4
        const val SETSTRING = 5

        /**
         * Parse a [LogRecord] from a [BasicLogRecord]. Dual of [writeToLog].
         */
        operator fun invoke(record: BasicLogRecord): LogRecord {
            val type = record.nextInt()
            return when (type) {
                CHECKPOINT -> CheckPointRecord.from(record)
                START -> StartRecord.from(record)
                COMMIT -> CommitRecord.from(record)
                ROLLBACK -> RollbackRecord.from(record)
                SETINT -> SetIntRecord.from(record)
                SETSTRING -> SetStringRecord.from(record)
                else -> error("invalid record type: $type")
            }
        }
    }
}

/**
 * Checkpoints represent a point in time where there are no running transactions and
 * all changes have been flushed to disk. When performing recovery and undoing uncommitted
 * changes, we can stop once we reach a checkpoint.
 */
class CheckPointRecord : LogRecord() {

    override val txNumber: TransactionNumber?
        get() = null

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(CHECKPOINT)

    companion object {

        fun from(@Suppress("UNUSED_PARAMETER") rec: BasicLogRecord): CheckPointRecord {
            return CheckPointRecord()
        }
    }
}

/**
 * Marks the beginning of a transaction.
 */
class StartRecord(override val txNumber: TransactionNumber) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(START, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): StartRecord {
            val tx = TransactionNumber(rec.nextInt())
            return StartRecord(tx)
        }
    }
}

/**
 * Marks a transaction as committed.
 */
class CommitRecord(override val txNumber: TransactionNumber) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(COMMIT, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): CommitRecord {
            val tx = TransactionNumber(rec.nextInt())
            return CommitRecord(tx)
        }
    }
}

/**
 * Marks a transaction as rolled back.
 */
class RollbackRecord(override val txNumber: TransactionNumber) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(ROLLBACK, txNumber)

    companion object {

        fun from(rec: BasicLogRecord): RollbackRecord {
            val tx = TransactionNumber(rec.nextInt())
            return RollbackRecord(tx)
        }
    }
}

/**
 * Undo record for changing an int.
 */
class SetIntRecord(
        override val txNumber: TransactionNumber,
        private val block: database.layer_0_file.Block,
        private val offset: Int,
        private val oldValue: Int
    ) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(SETINT, txNumber, block.filename, block.number, offset, oldValue)

    override fun undo(txnum: TransactionNumber, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setValue(offset, SqlInt(oldValue), txnum, LogSequenceNumber.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetIntRecord {
            val tx = TransactionNumber(rec.nextInt())
            val filename = FileName(rec.nextString())
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextInt()

            return SetIntRecord(tx, database.layer_0_file.Block(filename, blockNum), offset, value)
        }
    }
}

/**
 * Undo record for changing a string.
 */
class SetStringRecord(
        override val txNumber: TransactionNumber,
        private val block: database.layer_0_file.Block,
        private val offset: Int,
        private val odValue: String
    ) : LogRecord() {

    override fun writeToLog(logManager: LogManager): LogSequenceNumber =
        logManager.append(SETSTRING, txNumber, block.filename, block.number, offset, odValue)

    override fun undo(txnum: TransactionNumber, bufferManager: BufferManager) {
        val buffer = bufferManager.pin(block)
        buffer.setValue(offset, SqlString(odValue), txnum, LogSequenceNumber.undefined)
        bufferManager.unpin(buffer)
    }

    companion object {

        fun from(rec: BasicLogRecord): SetStringRecord {
            val tx = TransactionNumber(rec.nextInt())
            val filename = FileName(rec.nextString())
            val blockNum = rec.nextInt()
            val offset = rec.nextInt()
            val value = rec.nextString()

            return SetStringRecord(tx, database.layer_0_file.Block(filename, blockNum), offset, value)
        }
    }
}
