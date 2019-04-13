package database.layer_5_metadata

import database.layer_5_metadata.TableManager.Companion.MAX_NAME
import database.layer_6_query.SqlString
import database.layer_4_record.*
import database.layer_3_c_tx.Transaction
import database.types.ColumnName
import database.types.IndexName
import database.types.TableName

/**
 * Class responsible for maintaining indices.
 */
class IndexManager(
    isNew: Boolean,
    tableManager: TableManager,
    private val metadataManager: MetadataManager,
    tx: Transaction
) {
    private val ti: TableInfo

    init {
        if (isNew) {
            tableManager.createTable(TABLE_INDEX_CAT, Schema {
                stringField(COLUMN_INDEX_NAME, MAX_NAME)
                stringField(COLUMN_TABLE_NAME, MAX_NAME)
                stringField(COLUMN_FIELD_NAME, MAX_NAME)
            }, tx)
        }

        ti = tableManager.getTableInfo(TABLE_INDEX_CAT, tx)
    }

    fun createIndex(indexName: IndexName, tableName: TableName, fieldName: ColumnName, tx: Transaction) {
        TableManager.checkNameLength(indexName.value, COLUMN_INDEX_NAME)
        TableManager.checkNameLength(tableName.value, COLUMN_TABLE_NAME)
        TableManager.checkNameLength(fieldName.value, COLUMN_FIELD_NAME)

        ti.open(tx).use { rf ->
            rf.insertRow(
                COLUMN_INDEX_NAME to SqlString(indexName.value),
                COLUMN_TABLE_NAME to SqlString(tableName.value),
                COLUMN_FIELD_NAME to SqlString(fieldName.value)
            )
        }
    }

    fun getIndexInfo(tableName: TableName, tx: Transaction): Map<ColumnName, IndexInfo> {
        ti.open(tx).use { rf ->
            val result = mutableMapOf<ColumnName, IndexInfo>()
            rf.forEach {
                if (rf.getString(COLUMN_TABLE_NAME) == tableName.value) {
                    val indexName = IndexName(rf.getString(COLUMN_INDEX_NAME))
                    val fieldName = ColumnName(rf.getString(COLUMN_FIELD_NAME))
                    result[fieldName] = IndexInfo(indexName, tableName, fieldName, tx, metadataManager)
                }
            }
            return result
        }
    }

    companion object {
        private val TABLE_INDEX_CAT = TableName("idxcat")
        private val COLUMN_INDEX_NAME = ColumnName("indexName")
        private val COLUMN_TABLE_NAME = ColumnName("tableName")
        private val COLUMN_FIELD_NAME = ColumnName("fieldName")
    }
}
