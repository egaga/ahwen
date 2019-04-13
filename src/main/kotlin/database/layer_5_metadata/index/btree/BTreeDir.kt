package database.layer_5_metadata.index.btree

import database.layer_3_c_tx.Transaction
import database.layer_4_record.TableInfo
import database.layer_6_query.SqlValue

/**
 * Represents a directory node of a B-Tree.
 */
class BTreeDir(
        block: database.layer_0_file.Block,
        private val ti: TableInfo,
        private val tx: Transaction
) {

    private val fileName = ti.fileName
    private var contents = BTreePage(block, ti, tx)

    fun close() {
        contents.close()
    }

    fun search(searchKey: SqlValue): Int {
        var childBlock = findChildBlock(searchKey)
        while (contents.flag > 0) {
            contents.close()
            contents = BTreePage(childBlock, ti, tx)
            childBlock = findChildBlock(searchKey)
        }
        return childBlock.number
    }

    fun makeNewRoot(e: DirEntry) {
        val firstVal = contents.getDataValue(0)
        val level = contents.flag
        val newBlock = contents.split(0, level)
        val oldRoot = DirEntry(firstVal, newBlock.number)
        insertEntry(oldRoot)
        insertEntry(e)
        contents.flag = level + 1
    }

    fun insert(e: DirEntry): DirEntry? {
        if (contents.flag == 0)
            return insertEntry(e)

        val childBlock = findChildBlock(e.dataval)
        val child = BTreeDir(childBlock, ti, tx)
        val myentry = child.insert(e)
        child.close()
        return if (myentry != null) insertEntry(myentry) else null
    }

    private fun insertEntry(e: DirEntry): DirEntry? {
        val newSlot = 1 + contents.findSlotBefore(e.dataval)
        contents.insertDir(newSlot, e.dataval, e.blocknum)
        if (!contents.isFull)
            return null

        val level = contents.flag
        val splitPos = contents.numRecs / 2
        val splitVal = contents.getDataValue(splitPos)
        val newBlock = contents.split(splitPos, level)

        return DirEntry(splitVal, newBlock.number)
    }

    private fun findChildBlock(searchKey: SqlValue): database.layer_0_file.Block {
        var slot = contents.findSlotBefore(searchKey)
        if (contents.getDataValue(slot + 1) == searchKey)
            slot++
        val blockNum = contents.getChildNum(slot)
        return database.layer_0_file.Block(fileName, blockNum)
    }
}
