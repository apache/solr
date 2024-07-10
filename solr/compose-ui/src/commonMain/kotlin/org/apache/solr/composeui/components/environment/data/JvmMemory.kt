package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class JvmMemory(
    val free: String = "",
    val total: String = "",
    val max: String = "",
    val used: String = "",
    val raw: JvmMemoryRaw = JvmMemoryRaw(),
)
