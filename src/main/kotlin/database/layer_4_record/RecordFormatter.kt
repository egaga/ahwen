package database.layer_4_record

import database.layer_2_buffer.PageFormatter
import database.layer_0_file.Page
import database.layer_0_file.Page.Companion.BLOCK_SIZE
import database.layer_0_file.set

/**
 * [PageFormatter] that initializes the page with empty records of type [ti].
 */
class RecordFormatter(private val ti: TableInfo) : PageFormatter {

    override fun format(page: Page) {
        val recordSize = ti.recordLength + Int.SIZE_BYTES

        var position = 0
        while (position + recordSize <= BLOCK_SIZE) {
            page[position] = RecordPage.EMPTY
            makeDefaultRecord(page, position)
            position += recordSize
        }
    }

    private fun makeDefaultRecord(page: Page, pos: Int) {
        for (column in ti.schema.columns) {
            val offset = ti.offset(column)
            val position = pos + Int.SIZE_BYTES + offset
            val type = ti.schema.type(column)
            page[position] = type.defaultValue
        }
    }
}
