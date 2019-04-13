package database.layer_6_query.index

import database.layer_5_metadata.index.Index
import database.layer_6_query.SqlValue
import database.layer_6_query.Scan
import database.layer_6_query.TableScan
import database.types.ColumnName

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
