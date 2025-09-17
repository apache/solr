package org.apache.solr.ui.components.collections.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class CollectionsList(
    @SerialName("collections")
    val collectionsList: List<String> = emptyList(),
)
