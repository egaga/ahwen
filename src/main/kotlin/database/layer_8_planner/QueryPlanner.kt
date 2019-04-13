package database.layer_8_planner

import database.layer_7_parse.QueryData
import database.layer_6_query.Plan
import database.layer_3_c_tx.Transaction

/**
 * Query planners take an AST representing the original SQL query and turn that
 * into an executable plan.
 */
interface QueryPlanner {
    fun createPlan(data: QueryData, tx: Transaction): Plan
}
