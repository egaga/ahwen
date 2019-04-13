package database.layer_1_log

/**
 * Log sequence number
 *
 * Can be used to guarantee that log is flushed until certain point in time.
 */
data class LogSequenceNumber(val lsn: Int) : Comparable<LogSequenceNumber> {

    override fun compareTo(other: LogSequenceNumber): Int =
        compareValues(lsn, other.lsn)

    companion object {
        val zero = LogSequenceNumber(0)
        val undefined = LogSequenceNumber(-1)
    }
}
