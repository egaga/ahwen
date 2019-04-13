package database.layer_2_buffer

/**
 * Exception thrown when pinning a buffer failed because of timeout or deadlock.
 */
class BufferAbortException : RuntimeException()
