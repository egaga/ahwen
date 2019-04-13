package database.layer_5_metadata

import database.layer_4_record.Schema
import database.layer_4_record.TableInfo
import database.layer_3_c_tx.Transaction
import database.types.ColumnName
import database.types.IndexName
import database.types.TableName

/**
 * A facade to database metadata.
 */
class MetadataManager(isNew: Boolean, tx: Transaction) {

    private val tableManager = TableManager(isNew, tx)
    private val viewManager = ViewManager(isNew, tableManager, tx)
    private val indexManager = IndexManager(isNew, tableManager, this, tx)
    private val statManager = StatManager(tableManager, tx)

    fun createTable(name: TableName, schema: Schema, tx: Transaction) {
        tableManager.createTable(name, schema, tx)
    }

    fun getTableInfo(name: TableName, tx: Transaction) =
        tableManager.getTableInfo(name, tx)

    fun createView(name: TableName, def: String, tx: Transaction) {
        viewManager.createView(name, def, tx)
    }

    fun getViewDef(name: TableName, tx: Transaction): String? =
        viewManager.getViewDef(name, tx)

    fun createIndex(indexName: IndexName, tableName: TableName, columnName: ColumnName, tx: Transaction) {
        indexManager.createIndex(indexName, tableName, columnName, tx)
    }

    fun getIndexInfo(tableName: TableName, tx: Transaction): Map<ColumnName, IndexInfo> =
        indexManager.getIndexInfo(tableName, tx)

    fun getStatInfo(tableName: TableName, ti: TableInfo, tx: Transaction): StatInfo =
        statManager.getStatInfo(tableName, ti, tx)
}
