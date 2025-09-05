package org.apache.solr.ui.components.collections.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ListConfigSets(
    @SerialName("configSets")
    val names: List<String> = emptyList(),
)
