package database.layer_9_jdbc

import database.utils.unimplemented
import java.io.File
import java.sql.Connection
import java.sql.Statement

class AhwenConnection(dir: File) : Connection by unimplemented() {

    val db = AhwenDatabase(dir)

    override fun createStatement(): Statement =
        AhwenStatement(db)

    override fun getAutoCommit(): Boolean =
        true

    override fun setAutoCommit(autoCommit: Boolean) {
        error("currently only auto-commit is supported")
    }
}
