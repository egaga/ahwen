package dev.komu.ahwen.layer_6_query

import dev.komu.ahwen.layer_4_record.RID
import dev.komu.ahwen.layer_4_record.TableInfo
import dev.komu.ahwen.layer_3_c_tx.Transaction
import dev.komu.ahwen.types.ColumnName

/**
 * Runtime implementation of [TablePlan].
 */
class TableScan(ti: TableInfo, tx: Transaction) : UpdateScan {

    private val rf = ti.open(tx)
    private val schema = ti.schema

    override fun beforeFirst() {
        rf.beforeFirst()
    }

    override fun next(): Boolean =
        rf.next()

    override fun close() {
        rf.close()
    }

    override fun get(column: ColumnName) =
        rf.getValue(column)

    override fun contains(column: ColumnName): Boolean =
        column in schema

    override fun set(column: ColumnName, value: SqlValue) =
        rf.setValue(column, value)

    override fun delete() {
        rf.delete()
    }

    override fun insert() {
        rf.insert()
    }

    override val rid: RID
        get() = rf.currentRid

    override fun moveToRid(rid: RID) {
        rf.moveToRid(rid)
    }
}
