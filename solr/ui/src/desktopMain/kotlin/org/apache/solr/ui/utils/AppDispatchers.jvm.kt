package org.apache.solr.ui.utils

import kotlinx.coroutines.Dispatchers

actual fun platformDispatchers(): AppDispatchers = object : AppDispatchers {
    override val io = Dispatchers.IO
}
