package dev.komu.ahwen.layer_3_b_tx.concurrency

/**
 * Exception thrown when locking fails because of a timeout or deadlock.
 */
class LockAbortException : RuntimeException()
