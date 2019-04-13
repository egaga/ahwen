package database.layer_5_metadata.index.btree

import database.layer_6_query.SqlValue

class DirEntry(val dataval: SqlValue, val blocknum: Int)
