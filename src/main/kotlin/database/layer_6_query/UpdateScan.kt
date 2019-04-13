package database.layer_6_query

import database.layer_4_record.RID
import database.layer_4_record.Schema
import database.types.ColumnName

/**
 * A [Scan] that supports updating.
 */
interface UpdateScan : Scan {

    /**
     * Set the value of [column] in current row to [value].
     */
    operator fun set(column: ColumnName, value: SqlValue)

    /**
     * Insert a new row and change position into it.
     */
    fun insert()

    /**
     * Delete the current row.
     */
    fun delete()

    /**
     * Returns the id of current row.
     */
    val rid: RID

    /**
     * Change position to current row.
     */
    fun moveToRid(rid: RID)
}

/**
 * Copy all rows from [sourcePlan] to this scan.
 */
fun UpdateScan.copyFrom(sourcePlan: Plan, schema: Schema) {
    sourcePlan.open().use { src ->
        copyFrom(src, schema)
    }
}

/**
 * Copy all remaining rows from [sourceScan] to this scan.
 */
fun UpdateScan.copyFrom(sourceScan: Scan, schema: Schema) {
    while (sourceScan.next())
        insertRowFrom(sourceScan, schema)
}

/**
 * Insert a new row with values taken from current row of [source].
 */
fun UpdateScan.insertRowFrom(source: Scan, schema: Schema) {
    insert()
    for (column in schema.columns)
        this[column] = source[column]
}
