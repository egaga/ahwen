package database.layer_8_planner

import database.layer_2_buffer.BufferManager
import database.layer_5_metadata.MetadataManager
import database.layer_7_parse.QueryData
import database.layer_7_parse.SelectExp
import database.layer_6_query.Plan
import database.layer_6_query.ProjectPlan
import database.layer_6_query.aggregates.AggregationFn
import database.layer_6_query.aggregates.CountFn
import database.layer_6_query.aggregates.GroupByPlan
import database.layer_6_query.aggregates.MaxFn
import database.layer_6_query.materialize.SortPlan
import database.layer_3_c_tx.Transaction
import database.types.ColumnName

/**
 * [QueryPlanner] that uses simple heuristics to decide on a good join order for tables:
 *
 * First the table that would result in lowest number of outputted rows is picked as the initial table.
 * Then as long as there are more tables available, joins the one whose output would result in the lowest
 * amount of rows outputted after the join.
 */
class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager,
    private val bufferManager: BufferManager
) : QueryPlanner {

    override fun createPlan(data: QueryData, tx: Transaction): Plan {
        // TODO: support views
        val tablePlanners = data.tables.map { TablePlanner(it, data.predicate, tx, metadataManager, bufferManager) }
            .toMutableList()

        var currentPlan = tablePlanners.getLowestSelectPlan()

        while (!tablePlanners.isEmpty())
            currentPlan = tablePlanners.getLowestJoinPlan(currentPlan) ?: tablePlanners.getLowestProductPlan(currentPlan)

        val aggregates = data.selectExps.filterIsInstance<SelectExp.Aggregate>().map { createAggregateFn(it.fn, it.column) }

        if (data.groupBy.isNotEmpty()) {
            currentPlan = GroupByPlan(currentPlan, data.groupBy, aggregates, tx)
        } else {
            check(aggregates.isEmpty()) { "specified aggregate functions without group by"}
        }

        if (data.orderBy.isNotEmpty())
            currentPlan = SortPlan(currentPlan, data.orderBy, tx)

        val columns = data.selectExps.filterIsInstance<SelectExp.Column>().map { it.column } + aggregates.map { it.columnName }

        return ProjectPlan(currentPlan, columns)
    }

    companion object {

        private fun createAggregateFn(name: String, column: ColumnName): AggregationFn = when (name) {
            "count" -> CountFn(column)
            "max" -> MaxFn(column)
            else -> error("unknown aggregate function '$name'")
        }

        private fun MutableList<TablePlanner>.getLowestSelectPlan(): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeSelectPlan() }
                .minBy { (_, plan) -> plan.recordsOutput } ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestJoinPlan(currentPlan: Plan): Plan? {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeJoinPlan(currentPlan) }
                .filter { (_, plan) -> plan != null }
                .minBy { (_, plan) -> plan!!.recordsOutput }
                ?: return null

            remove(bestPlanner)
            return bestPlan
        }

        private fun MutableList<TablePlanner>.getLowestProductPlan(currentPlan: Plan): Plan {
            val (bestPlanner, bestPlan) = this
                .map { it to it.makeProductPlan(currentPlan) }
                .minBy { (_, plan) -> plan.recordsOutput }
                ?: error("no planners")

            remove(bestPlanner)
            return bestPlan
        }
    }
}
