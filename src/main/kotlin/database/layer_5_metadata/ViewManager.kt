package database.layer_5_metadata

import database.layer_5_metadata.TableManager.Companion.checkNameLength
import database.layer_6_query.SqlString
import database.layer_4_record.*
import database.layer_3_c_tx.Transaction
import database.types.ColumnName
import database.types.TableName

/**
 * Class responsible for maintaining views.
 */
class ViewManager(isNew: Boolean, private val tableManager: TableManager, tx: Transaction) {

    init {
        if (isNew) {
            val schema = Schema {
                stringField(COL_VIEW_NAME, TableManager.MAX_NAME)
                stringField(COL_VIEW_DEF, MAX_VIEWDEF)
            }
            tableManager.createTable(TBL_VIEW_CAT, schema, tx)
        }
    }

    fun createView(name: TableName, def: String, tx: Transaction) {
        checkNameLength(name.value, COL_VIEW_NAME)
        tableManager.getTableInfo(TBL_VIEW_CAT, tx).open(tx).use { rf ->
            rf.insertRow(
                COL_VIEW_NAME to SqlString(name.value),
                COL_VIEW_DEF to SqlString(def)
            )
        }
    }

    fun getViewDef(name: TableName, tx: Transaction): String? {
        tableManager.getTableInfo(TBL_VIEW_CAT, tx).open(tx).use { rf ->
            rf.forEach {
                if (rf.getString(COL_VIEW_NAME) == name.value)
                    return rf.getString(COL_VIEW_DEF)
            }
            return null
        }
    }

    companion object {

        private val TBL_VIEW_CAT = TableName("viewcat")
        private val COL_VIEW_NAME = ColumnName("viewname")
        private val COL_VIEW_DEF = ColumnName("viewdef")

        private const val MAX_VIEWDEF = 100
    }
}
