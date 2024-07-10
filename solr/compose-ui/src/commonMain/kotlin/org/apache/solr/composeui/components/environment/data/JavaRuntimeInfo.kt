package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class JavaRuntimeInfo(
    val version: String = "",
    val vendor: String = "",
    val name: String? = null,
)
