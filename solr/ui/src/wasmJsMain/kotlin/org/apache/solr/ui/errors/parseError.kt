package org.apache.solr.ui.errors

actual fun parseError(error: Throwable): Throwable {
    // In JavaScript the errors do not have any type to distinguish them, so we strongly
    // relay on the error message.
    if (error.message?.contains(other = "Fail to fetch") == true) return HostNotFoundException(cause = error)

    // fallback
    return error
}
