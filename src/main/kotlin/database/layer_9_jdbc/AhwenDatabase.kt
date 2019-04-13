package database.layer_9_jdbc

import database.layer_1_log.LogManager
import database.layer_2_buffer.BufferManager
import database.layer_3_b_tx.concurrency.LockTable
import database.layer_3_c_tx.Transaction
import database.layer_5_metadata.MetadataManager
import database.layer_8_planner.Planner
import database.types.FileName
import java.io.File

class AhwenDatabase(dir: File) {
    private val fileManager = database.layer_0_file.DefaultFileManager(dir)
    private val logManager = LogManager(fileManager, FileName("log"))
    private val bufferManager = BufferManager(1000, fileManager, logManager)
    private val lockTable = LockTable()
    private val metadataManager: MetadataManager
    val planner: Planner

    val fileStats: database.layer_0_file.FileStats
        get() = fileManager.stats

    init {
        Transaction(logManager, bufferManager, lockTable, fileManager).also { tx ->
            tx.recover()

            metadataManager = MetadataManager(fileManager.isNew, tx)
            tx.commit()
        }
        planner = Planner(metadataManager, bufferManager)
    }

    fun beginTransaction() =
        Transaction(logManager, bufferManager, lockTable, fileManager)
}
