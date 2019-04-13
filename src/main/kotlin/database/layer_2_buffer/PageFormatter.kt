package database.layer_2_buffer

import database.layer_0_file.Page

/**
 * Strategy for initializing a newly allocated [Page].
 */
interface PageFormatter {
    fun format(page: Page)
}
