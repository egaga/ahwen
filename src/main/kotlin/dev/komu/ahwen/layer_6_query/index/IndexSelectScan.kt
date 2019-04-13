package dev.komu.ahwen.layer_6_query.index

import dev.komu.ahwen.layer_5_metadata.index.Index
import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.Scan
import dev.komu.ahwen.layer_6_query.TableScan
import dev.komu.ahwen.types.ColumnName

/**
 * Runtime implementation of [IndexSelectPlan].
 */
class IndexSelectScan(
    private val index: Index,
    private val value: SqlValue,
    private val tableScan: TableScan
) : Scan {

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        index.beforeFirst(value)
    }

    override fun next(): Boolean {
        val ok = index.next()
        if (ok)
            tableScan.moveToRid(index.dataRid)
        return ok
    }

    override fun close() {
        index.close()
        tableScan.close()
    }

    override fun get(column: ColumnName): SqlValue =
        tableScan[column]

    override fun contains(column: ColumnName): Boolean =
        column in tableScan
}
