package dev.komu.ahwen.layer_6_query.aggregates

import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.Scan
import dev.komu.ahwen.types.ColumnName

/**
 * Interface for aggregate functions.
 *
 * The function is instantiated only once, not once per each aggregate group. For each group
 * that is processed together, following calls will be made:
 *
 * - [processFirst] is called for the first row to start a new group
 * - [processNext] is called for each of the successive rows
 * - [value] is to retrieve the computed value for the group
 */
interface AggregationFn {

    /** Called for the first row of each new group. */
    fun processFirst(scan: Scan)

    /** Called for successive rows of each group. */
    fun processNext(scan: Scan)

    /** Returns the final aggregated value. */
    val value: SqlValue

    /** Column name for the results this group produces. */
    val columnName: ColumnName
}
