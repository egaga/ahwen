package database.layer_6_query.materialize

import database.layer_0_file.Page.Companion.BLOCK_SIZE
import database.layer_6_query.Plan
import database.layer_6_query.Scan
import database.layer_6_query.copyFrom
import database.layer_4_record.TableInfo
import database.layer_3_c_tx.Transaction
import database.types.TableName
import kotlin.math.ceil

/**
 * A plan that materializes given plan, i.e. writes it to a temporary table
 * so that we don't need to recalculate it over and over again.
 */
class MaterializePlan(private val srcPlan: Plan, private val tx: Transaction) : Plan by srcPlan {

    override fun open(): Scan {
        val schema = srcPlan.schema
        val temp = TempTable(schema, tx)
        val dest = temp.open()
        dest.copyFrom(srcPlan, schema)
        dest.beforeFirst()
        return dest
    }

    override val blocksAccessed: Int
        get() {
            val ti = TableInfo(TableName.DUMMY, srcPlan.schema)
            val rpb = (BLOCK_SIZE / ti.recordLength).toDouble()
            return ceil(srcPlan.recordsOutput / rpb).toInt()
        }
}
