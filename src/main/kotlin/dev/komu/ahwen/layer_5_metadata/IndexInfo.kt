package dev.komu.ahwen.layer_5_metadata

import dev.komu.ahwen.layer_0_file.Page.Companion.BLOCK_SIZE
import dev.komu.ahwen.layer_5_metadata.index.Index
import dev.komu.ahwen.layer_5_metadata.index.btree.BTreeIndex
import dev.komu.ahwen.layer_5_metadata.index.btree.BTreePage
import dev.komu.ahwen.layer_4_record.Schema
import dev.komu.ahwen.layer_3_c_tx.Transaction
import dev.komu.ahwen.types.ColumnName
import dev.komu.ahwen.types.IndexName
import dev.komu.ahwen.types.SqlType
import dev.komu.ahwen.types.TableName

/**
 * Describes an index.
 *
 * Contains statistics about the index to guide planning decision. Also contains metadata
 * about the structure of the index so that it can be opened for scanning.
 */
class IndexInfo(
    private val indexName: IndexName,
    tableName: TableName,
    private val fieldName: ColumnName,
    private val tx: Transaction,
    metadataManager: MetadataManager
) {

    private val ti = metadataManager.getTableInfo(tableName, tx)
    private val si = metadataManager.getStatInfo(tableName, ti, tx)

    fun open(): Index =
        BTreeIndex(indexName, schema(), tx)

    /**
     * Estimate how many blocks must be accessed to perform a load from index.
     */
    val blocksAccessed: Int
        get() {
            val rpb = BLOCK_SIZE / ti.recordLength
            val blockCount = si.numRecords / rpb
            return BTreeIndex.searchCost(blockCount, rpb)
        }

    /**
     * Estimate how many rows the output of index query will return.
     */
    val recordsOutput: Int
        get() = si.numRecords / si.distinctValues(fieldName)

    /**
     * Estimate how many distinct values index has for given field.
     */
    fun distinctValues(fieldName: ColumnName): Int =
        if (fieldName == this.fieldName)
            1
        else
            minOf(si.distinctValues(fieldName), recordsOutput)

    private fun schema() = Schema {
        intField(BTreePage.COL_BLOCK)
        intField(BTreePage.COL_ID)
        val info = ti.schema[fieldName]
        when (info.type) {
            SqlType.INTEGER ->
                intField(BTreePage.COL_DATAVAL)
            SqlType.VARCHAR ->
                stringField(BTreePage.COL_DATAVAL, info.length)
        }
    }

    override fun toString() = "[Index name=$indexName, field$fieldName]"
}
