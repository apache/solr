package org.apache.solr.ui.components.configsets.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * List of config sets.
 */
@Serializable
data class ListConfigSets(
    @SerialName("configSets")
    val names: List<String> = emptyList(),
)
