package dev.komu.ahwen.layer_0_file

import dev.komu.ahwen.types.FileName

/**
 * Uniquely identifies a block in some file.
 *
 * Blocks can be loaded to [Page]s for accessing their data.
 */
data class Block(val filename: FileName, val number: Int)
