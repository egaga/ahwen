package dev.komu.ahwen.metadata

import dev.komu.ahwen.file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.index.Index
import dev.komu.ahwen.index.btree.BTreeIndex
import dev.komu.ahwen.record.Schema
import dev.komu.ahwen.tx.Transaction
import dev.komu.ahwen.types.SqlType

class IndexInfo(
    private val indexName: String,
    tableName: String,
    private val fieldName: String,
    private val tx: Transaction,
    metadataManager: MetadataManager
) {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    fun open(): Index =
        BTreeIndex(indexName, schema(), tx)

    val blocksAccessed: Int
        get() {
            val rpb = BLOCK_SIZE / ti.recordLength
            val blockCount = si.numRecords / rpb
            return BTreeIndex.searchCost(blockCount, rpb)
        }

    val recordsOutput: Int
        get() = si.numRecords / si.distinctValues(fieldName)

    fun distinctValues(fieldName: String): Int =
        if (fieldName == this.fieldName)
            1
        else
            minOf(si.distinctValues(fieldName), recordsOutput)

    private fun schema() = Schema {
        intField("block")
        intField("id")
        val type = ti.schema.type(fieldName)
        when (type) {
            SqlType.INTEGER ->
                intField("dataval")
            SqlType.VARCHAR ->
                stringField("dataval", ti.schema.length(fieldName))
        }
    }

    override fun toString() = "[Index name=$indexName, field$fieldName]"
}
