package dev.komu.ahwen.layer_4_record

import dev.komu.ahwen.layer_0_file.Block
import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.SqlInt
import dev.komu.ahwen.layer_6_query.SqlString
import dev.komu.ahwen.layer_3_c_tx.Transaction
import dev.komu.ahwen.types.ColumnName
import java.io.Closeable

/**
 * A cursor for reading records described by [ti] from a file.
 */
class RecordFile(private val ti: TableInfo, private val tx: Transaction) : Closeable {

    private val filename = ti.fileName
    private var recordPage: RecordPage
    private var currentBlockNum = 0

    init {
        if (tx.size(filename) == 0)
            appendBlock()
        recordPage = RecordPage(Block(filename, currentBlockNum), ti, tx)
    }

    override fun close() {
        recordPage.close()
    }

    fun beforeFirst() {
        moveTo(0)
    }

    fun next(): Boolean {
        while (true) {
            if (recordPage.next())
                return true

            if (currentlyAtLastBlock)
                return false

            moveTo(currentBlockNum + 1)
        }
    }

    fun getValue(column: ColumnName) =
        recordPage.getValue(column, ti.schema.type(column))

    fun setValue(column: ColumnName, value: SqlValue) {
        recordPage.setValue(column, value)
    }

    fun delete() {
        recordPage.delete()
    }

    fun insert() {
        while (!recordPage.insert()) {
            if (currentlyAtLastBlock)
                appendBlock()

            moveTo(currentBlockNum + 1)
        }
    }

    fun moveToRid(rid: RID) {
        moveTo(rid.blockNumber)
        recordPage.moveToId(rid.id)
    }

    val currentRid: RID
        get() = RID(currentBlockNum, recordPage.currentId)

    private fun moveTo(b: Int) {
        recordPage.close()

        currentBlockNum = b
        recordPage = RecordPage(Block(filename, currentBlockNum), ti, tx)
    }

    private val currentlyAtLastBlock: Boolean
        get() = currentBlockNum == tx.size(filename) - 1

    private fun appendBlock() {
        tx.append(filename, RecordFormatter(ti))
    }
}

inline fun RecordFile.forEach(func: () -> Unit) {
    while (next())
        func()
}

fun RecordFile.getInt(column: ColumnName) =
    (getValue(column) as SqlInt).value

fun RecordFile.getString(column: ColumnName) =
    (getValue(column) as SqlString).value

fun RecordFile.insertRow(vararg values: Pair<ColumnName, SqlValue>) {
    insert()
    for ((column, value) in values)
        setValue(column, value)
}
