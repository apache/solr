package org.apache.solr.ui.components.collections.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.store.CollectionsStore.Intent
import org.apache.solr.ui.components.collections.store.CollectionsStore.State

internal interface CollectionsStore : Store<Intent, State, Nothing> {

    sealed interface Intent {

        /**
         * Intent for requesting Collections list.
         */
        data object FetchCollectionList : Intent

        /**
         * Represents an intent to fetch data related to a specific collection.
         *
         * @property name The name of the collection to fetch data */
        data class FetchCollectionData(val name: String) : Intent
    }

    data class State(
        val collectionsList: CollectionsList = CollectionsList(),
        val selectedCollectionData: CollectionData? = null,
    )
}
