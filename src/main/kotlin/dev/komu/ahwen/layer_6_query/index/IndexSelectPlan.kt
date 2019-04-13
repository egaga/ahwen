package dev.komu.ahwen.layer_6_query.index

import dev.komu.ahwen.layer_5_metadata.IndexInfo
import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.Plan
import dev.komu.ahwen.layer_6_query.Scan
import dev.komu.ahwen.layer_6_query.TablePlan
import dev.komu.ahwen.layer_4_record.Schema
import dev.komu.ahwen.types.ColumnName

/**
 * Select rows of table using index.
 */
class IndexSelectPlan(
    private val plan: TablePlan,
    private val indexInfo: IndexInfo,
    private val value: SqlValue
) : Plan {

    override fun open(): Scan =
        IndexSelectScan(indexInfo.open(), value, plan.open())

    override val blocksAccessed: Int
        get() = indexInfo.blocksAccessed + recordsOutput

    override val recordsOutput: Int
        get() = indexInfo.recordsOutput

    override fun distinctValues(column: ColumnName): Int =
        indexInfo.distinctValues(column)

    override val schema: Schema
        get() = plan.schema
}
