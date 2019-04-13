package database.layer_0_file

import database.types.FileName

/**
 * Uniquely identifies a block in some file.
 *
 * Blocks can be loaded to [Page]s for accessing their data.
 */
data class Block(val filename: FileName, val number: Int)
