package org.apache.solr.ui.components.collections

import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.components.collections.data.CollectionsList

/**
 * Component interface that represents the collection section.
 */
interface CollectionsComponent {

    val model: StateFlow<Model>

    data class Model(
        val collectionsList: CollectionsList = CollectionsList(),
        val selectedCollectionData: CollectionData? = null,
    )

    fun selectCollection(name: String)
}
