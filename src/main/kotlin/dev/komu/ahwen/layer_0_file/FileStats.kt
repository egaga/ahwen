package dev.komu.ahwen.layer_0_file

import java.util.concurrent.atomic.AtomicLong

/**
 * Statistics about block reads and writes performed by the database.
 */
class FileStats {

    private val _reads = AtomicLong(0)
    private val _writes = AtomicLong(0)

    val reads: Long
        get() = _reads.get()

    val writes: Long
        get() = _writes.get()

    fun incrementReads() {
        _reads.incrementAndGet()
    }

    fun incrementWrites() {
        _writes.incrementAndGet()
    }

    fun reset() {
        _reads.set(0)
        _writes.set(0)
    }

    override fun toString() = "reads: $reads, writes: $writes"
}
