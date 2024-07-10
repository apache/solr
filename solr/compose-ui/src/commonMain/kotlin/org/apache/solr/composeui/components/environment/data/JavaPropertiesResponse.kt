package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class JavaPropertiesResponse(
    @SerialName("system.properties")
    val properties: Map<String, String>,
)
