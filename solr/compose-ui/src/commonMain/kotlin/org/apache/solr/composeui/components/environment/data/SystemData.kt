package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class SystemData(
    val mode: SystemMode,
    val zkHost: String,
    @SerialName("solr_home")
    val solrHome: String,
    @SerialName("core_root")
    val coreRoot: String,
    val lucene: Versions,
    val jvm: JvmData,
    val security: SecurityConfig,
    val system: SystemInformation,
    val node: String,
)
