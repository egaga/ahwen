package database.layer_5_metadata.index.btree

import database.layer_2_buffer.PageFormatter
import database.layer_0_file.Page
import database.layer_0_file.Page.Companion.BLOCK_SIZE
import database.layer_0_file.set
import database.layer_4_record.TableInfo

/**
 * Formats an empty page ready to be used for a B Tree Index.
 */
class BTreePageFormatter(
    private val ti: TableInfo,
    private val flag: Int
) :
    PageFormatter {

    override fun format(page: Page) {
        page[0] = flag
        page[Int.SIZE_BYTES] = 0
        val recordSize = ti.recordLength

        var pos = 2 * Int.SIZE_BYTES
        while (pos + recordSize <= BLOCK_SIZE) {
            makeDefaultRecord(page, pos)
            pos += recordSize
        }
    }

    private fun makeDefaultRecord(page: Page, pos: Int) {
        for (column in ti.schema.columns) {
            val offset = ti.offset(column)
            val type = ti.schema.type(column)
            page[pos + offset] = type.defaultValue
        }
    }
}