package database.layer_6_query.aggregates

import database.layer_6_query.Plan
import database.layer_6_query.Scan
import database.layer_6_query.materialize.SortPlan
import database.layer_4_record.Schema
import database.layer_3_c_tx.Transaction
import database.types.ColumnName

/**
 * Plan for [GroupByScan].
 *
 * First sorts the input by group key and then calculates the aggregates on the fly.
 */
class GroupByPlan(
    plan: Plan,
    private val groupFields: List<ColumnName>,
    private val aggregationFns: Collection<AggregationFn>,
    tx: Transaction
) : Plan {

    private val sortPlan = SortPlan(plan, groupFields, tx)

    override val schema = Schema {
        for (field in groupFields)
            copyFieldFrom(field, plan.schema)
        for (fn in aggregationFns)
            intField(fn.columnName)
    }

    override fun open(): Scan =
        GroupByScan(sortPlan.open(), groupFields, aggregationFns)

    override val blocksAccessed: Int
        get() = sortPlan.blocksAccessed

    override val recordsOutput: Int
        get() {
            var result = 1
            for (fieldName in groupFields)
                result *= sortPlan.distinctValues(fieldName)
            return result
        }

    override fun distinctValues(column: ColumnName): Int =
        if (column in sortPlan.schema)
            sortPlan.distinctValues(column)
        else
            recordsOutput
}
