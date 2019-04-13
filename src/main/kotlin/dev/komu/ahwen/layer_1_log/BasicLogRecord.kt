package dev.komu.ahwen.layer_1_log

import dev.komu.ahwen.layer_0_file.Page
import dev.komu.ahwen.layer_6_query.SqlValue
import dev.komu.ahwen.layer_6_query.SqlInt
import dev.komu.ahwen.layer_6_query.SqlString
import dev.komu.ahwen.types.SqlType

/**
 * Provides unstructured low-level read access to log records.
 *
 * Higher level log parsing can be implemented on top of this.
 */
class BasicLogRecord(private val page: Page, private var position: Int) {

    fun nextInt(): Int =
        (nextValue(SqlType.INTEGER) as SqlInt).value

    fun nextString(): String =
        (nextValue(SqlType.VARCHAR) as SqlString).value

    private fun nextValue(type: SqlType): SqlValue {
        val result = page.getValue(position, type)
        position += result.representationSize
        return result
    }
}
