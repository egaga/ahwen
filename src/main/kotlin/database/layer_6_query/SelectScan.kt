package database.layer_6_query

import database.layer_4_record.RID
import database.types.ColumnName

/**
 * Runtime implementation of [SelectPlan].
 */
class SelectScan(private val scan: Scan, private val predicate: Predicate) : Scan, UpdateScan {

    override fun next(): Boolean {
        while (scan.next())
            if (predicate.isSatisfied(scan))
                return true
        return false
    }

    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun close() {
        scan.close()
    }

    override fun get(column: ColumnName): SqlValue =
        scan[column]

    override fun contains(column: ColumnName): Boolean =
        column in scan

    override fun set(column: ColumnName, value: SqlValue) {
        val us = scan as UpdateScan
        us[column] = value
    }

    override fun delete() {
        val us = scan as UpdateScan
        us.delete()
    }

    override fun insert() {
        val us = scan as UpdateScan
        us.insert()
    }

    override val rid: RID
        get() = (scan as UpdateScan).rid

    override fun moveToRid(rid: RID) {
        val us = scan as UpdateScan
        us.moveToRid(rid)
    }
}
