package dev.komu.ahwen.layer_8_planner

import dev.komu.ahwen.layer_5_metadata.MetadataManager
import dev.komu.ahwen.layer_7_parse.*
import dev.komu.ahwen.layer_6_query.SelectPlan
import dev.komu.ahwen.layer_6_query.TablePlan
import dev.komu.ahwen.layer_6_query.forEach
import dev.komu.ahwen.layer_3_c_tx.Transaction

/**
 * [UpdatePlanner] that also keeps the indices in sync.
 */
class IndexUpdatePlanner(private val metadataManager: MetadataManager) : UpdatePlanner {

    override fun executeInsert(data: InsertData, tx: Transaction): Int {
        val plan = TablePlan(data.table, metadataManager, tx)

        plan.open().use { scan ->
            scan.insert()
            val rid = scan.rid

            val indices = metadataManager.getIndexInfo(data.table, tx)

            for ((field, value) in data.fields.zip(data.values)) {
                scan[field] = value

                indices[field]?.open()?.use { index ->
                    index.insert(value, rid)
                }
            }
        }
        return 1
    }

    override fun executeDelete(data: DeleteData, tx: Transaction): Int {
        val tablePlan = TablePlan(data.table, metadataManager, tx)
        SelectPlan(tablePlan, data.predicate).open().use { scan ->
            val indices = metadataManager.getIndexInfo(data.table, tx)

            var count = 0
            scan.forEach {
                val rid = scan.rid

                for ((fieldName, indexInfo) in indices) {
                    indexInfo.open().use { index ->
                        index.delete(scan[fieldName], rid)
                    }
                }
                scan.delete()
                count++
            }
            return count
        }
    }

    override fun executeModify(data: ModifyData, tx: Transaction): Int {
        val table = TablePlan(data.table, metadataManager, tx)
        val select = SelectPlan(table, data.predicate)
        val indexInfo = metadataManager.getIndexInfo(data.table, tx)[data.fieldName]
        val index = indexInfo?.open()

        select.open().use { scan ->
            var count = 0
            scan.forEach {
                val newValue = data.newValue.evaluate(scan)
                val oldValue = scan[data.fieldName]
                scan[data.fieldName] = newValue

                if (index != null) {
                    val rid = scan.rid
                    index.delete(oldValue, rid)
                    index.insert(newValue, rid)
                }
                count++
            }

            index?.close()
            return count
        }
    }

    override fun executeCreateTable(data: CreateTableData, tx: Transaction): Int {
        metadataManager.createTable(data.table, data.schema, tx)
        return 0
    }

    override fun executeCreateView(data: CreateViewData, tx: Transaction): Int {
        metadataManager.createView(data.view, data.viewDefinition, tx)
        return 0
    }

    override fun executeCreateIndex(data: CreateIndexData, tx: Transaction): Int {
        metadataManager.createIndex(data.index, data.table, data.field, tx)
        return 0
    }

}
