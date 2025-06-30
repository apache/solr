package org.apache.solr.ui.errors

/**
 * Exception that is thrown when the client has failed to connect to a host.
 *
 * @property cause The root cause that caused this exception.
 */
class HostNotFoundException(cause: Throwable? = null): Throwable(cause)
