package database.layer_4_record

import database.layer_0_file.Page.Companion.BLOCK_SIZE
import database.layer_3_c_tx.Transaction
import database.layer_3_c_tx.getInt
import database.layer_3_c_tx.setInt
import database.layer_6_query.SqlValue
import database.types.ColumnName
import database.types.SqlType

/**
 * A cursor for reading records described by [ti] from a block.
 */
class RecordPage(
        private val block: database.layer_0_file.Block,
        private val ti: TableInfo,
        private val tx: Transaction
) {

    private val slotSize = ti.recordLength + Int.SIZE_BYTES
    var currentId = -1
        private set

    init {
        tx.pin(block)
    }

    fun close() {
        tx.unpin(block)
    }

    fun next() = searchFor(IN_USE)

    fun getValue(column: ColumnName, type: SqlType) =
        tx.getValue(block, columnPosition(column), type)

    fun setValue(column: ColumnName, value: SqlValue) {
        tx.setValue(block, columnPosition(column), value)
    }

    fun delete() {
        tx.setInt(block, currentPos, EMPTY)
    }

    fun insert(): Boolean {
        this.currentId = -1
        val found = searchFor(EMPTY)
        if (found)
            tx.setInt(block, currentPos, IN_USE)
        return found
    }

    fun moveToId(id: Int) {
        this.currentId = id
    }

    private val currentPos: Int
        get() = currentId * slotSize

    private fun columnPosition(name: ColumnName): Int {
        val offset = Int.SIZE_BYTES + ti.offset(name)
        return currentPos + offset
    }

    private val isValidSlot: Boolean
        get() = currentPos + slotSize <= BLOCK_SIZE

    private fun searchFor(flag: Int): Boolean {
        currentId++

        while (isValidSlot) {
            if (tx.getInt(block, currentPos) == flag)
                return true
            currentId++
        }

        return false
    }

    companion object {
        const val EMPTY = 0
        const val IN_USE = 1
    }
}
