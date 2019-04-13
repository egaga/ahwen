package dev.komu.ahwen.layer_2_buffer

import dev.komu.ahwen.layer_0_file.Page

/**
 * Strategy for initializing a newly allocated [Page].
 */
interface PageFormatter {
    fun format(page: Page)
}
