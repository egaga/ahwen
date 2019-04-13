package database.layer_6_query.aggregates

import database.layer_6_query.SqlValue
import database.layer_6_query.Scan
import database.types.ColumnName

/**
 * Calculates the maximum value of given column in group of rows.
 */
class MaxFn(private val column: ColumnName) : AggregationFn {

    override lateinit var value: SqlValue

    override fun processFirst(scan: Scan) {
        value = scan[column]
    }

    override fun processNext(scan: Scan) {
        value = maxOf(value, scan[column])
    }

    override val columnName = ColumnName("maxof$column")
}
