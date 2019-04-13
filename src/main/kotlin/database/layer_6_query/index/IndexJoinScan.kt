package database.layer_6_query.index

import database.layer_5_metadata.index.Index
import database.layer_6_query.SqlValue
import database.layer_6_query.Scan
import database.layer_6_query.TableScan
import database.types.ColumnName

/**
 * Runtime implementation of [IndexJoinPlan].
 */
class IndexJoinScan(
    private val scan: Scan,
    private val index: Index,
    private val joinField: ColumnName,
    private val tableScan: TableScan
) : Scan {

    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan.beforeFirst()
        scan.next()
        resetIndex()
    }

    override fun next(): Boolean {
        while (true) {
            if (index.next()) {
                tableScan.moveToRid(index.dataRid)
                return true
            }

            if (!scan.next())
                return false
            resetIndex()
        }
    }

    override fun close() {
        scan.close()
        tableScan.close()
        index.close()
    }

    override fun get(column: ColumnName): SqlValue =
        if (column in tableScan) tableScan[column] else scan[column]

    override fun contains(column: ColumnName): Boolean =
        column in tableScan || column in scan

    private fun resetIndex() {
        index.beforeFirst(scan[joinField])
    }
}
