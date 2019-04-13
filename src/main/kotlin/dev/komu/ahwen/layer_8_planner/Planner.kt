package dev.komu.ahwen.layer_8_planner

import dev.komu.ahwen.layer_2_buffer.BufferManager
import dev.komu.ahwen.layer_5_metadata.MetadataManager
import dev.komu.ahwen.layer_7_parse.*
import dev.komu.ahwen.layer_6_query.Plan
import dev.komu.ahwen.layer_3_c_tx.Transaction

/**
 * Facade for query planning.
 */
class Planner(metadataManager: MetadataManager, bufferManager: BufferManager) {

    private val queryPlanner = HeuristicQueryPlanner(metadataManager, bufferManager)
    private val updatePlanner = IndexUpdatePlanner(metadataManager)

    fun createQueryPlan(sql: String, tx: Transaction): Plan {
        val parser = Parser(sql)
        val data = parser.query()
        return queryPlanner.createPlan(data, tx)
    }

    fun executeUpdate(sql: String, tx: Transaction): Int {
        val parser = Parser(sql)
        val data = parser.updateCmd()
        return when (data) {
            is InsertData -> updatePlanner.executeInsert(data, tx)
            is DeleteData -> updatePlanner.executeDelete(data, tx)
            is ModifyData -> updatePlanner.executeModify(data, tx)
            is CreateTableData -> updatePlanner.executeCreateTable(data, tx)
            is CreateViewData -> updatePlanner.executeCreateView(data, tx)
            is CreateIndexData -> updatePlanner.executeCreateIndex(data, tx)
        }
    }
}
