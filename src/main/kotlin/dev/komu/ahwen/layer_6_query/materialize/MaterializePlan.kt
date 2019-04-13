package dev.komu.ahwen.layer_6_query.materialize

import dev.komu.ahwen.layer_0_file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.layer_6_query.Plan
import dev.komu.ahwen.layer_6_query.Scan
import dev.komu.ahwen.layer_6_query.copyFrom
import dev.komu.ahwen.layer_4_record.TableInfo
import dev.komu.ahwen.layer_3_c_tx.Transaction
import dev.komu.ahwen.types.TableName
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
