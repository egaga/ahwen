package database.layer_6_query.materialize

import database.layer_6_query.TableScan
import database.layer_6_query.UpdateScan
import database.layer_4_record.Schema
import database.layer_4_record.TableInfo
import database.layer_3_c_tx.Transaction
import database.types.TableName
import java.util.concurrent.atomic.AtomicInteger

/**
 * Represents a temporary table. Each instantiated table is distinct from all other tables.
 *
 * TODO: Currently the temporary tables are never released. Temporary tables will be deleted
 *       during system startup, but as long as the server is running, they will be kept.
 *       Relatively straightforward way to fix this would be to associate them with transactions
 *       and clear them when transaction finishes. However, for long running transactions this
 *       could cause the tables to be kept around longer than necessary.
 *
 * TODO: Currently temporary tables are just tables named `tempXXX` where XXX is a running number.
 *       Nothing prevents normal queries from accessing these same tables.
 */
class TempTable(schema: Schema, private val tx: Transaction) {

    val tableInfo = TableInfo(nextTableName(), schema)

    fun open(): UpdateScan =
        TableScan(tableInfo, tx)

    companion object {

        private val nextTableNum = AtomicInteger(0)

        private fun nextTableName() =
            TableName.temporary(nextTableNum.incrementAndGet())
    }
}
