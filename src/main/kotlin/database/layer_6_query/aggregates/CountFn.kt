package database.layer_6_query.aggregates

import database.layer_6_query.SqlValue
import database.layer_6_query.SqlInt
import database.layer_6_query.Scan
import database.types.ColumnName

/**
 * Count rows in a group.
 */
class CountFn(column: ColumnName) : AggregationFn {

    override val columnName = ColumnName("countof$column")

    private var count = 0

    override fun processFirst(scan: Scan) {
        count = 1
    }

    override fun processNext(scan: Scan) {
        count++
    }

    override val value: SqlValue
        get() = SqlInt(count)
}
