package org.apache.solr.ui.errors

import java.net.ConnectException

actual fun parseError(error: Throwable): Throwable {
    return when (error) {
        is ConnectException -> HostNotFoundException(error)
        else -> error
    }
}