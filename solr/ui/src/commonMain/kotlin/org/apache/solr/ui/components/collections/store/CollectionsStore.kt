package org.apache.solr.ui.components.collections.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.store.CollectionsStore.Intent
import org.apache.solr.ui.components.collections.store.CollectionsStore.State

interface CollectionsStore : Store<Intent, State, CollectionsStore.Label> {

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

        /**
         * Intent for requesting live nodes data.
         */
        data object FetchLiveNodesData : Intent

        /**
         * Intent for adding a replica to a collection.
         * @param nodeName The name of the node to add the replica to.
         * @param shardName The name of the shard to add the replica to.
         * @param type The type of the replica to add.
         */
        data class AddReplica(val nodeName: String?, val shardName: String, val type: String) : Intent

        /**
         * Represents an intent to delete a replica from a specific shard in a collection.
         *
         * @property shardName The name of the shard from which the replica should be deleted.
         * @property replicaName The name of the replica to be deleted.
         */
        data class DeleteReplica(val shardName: String, val replicaName: String) : Intent

        /**
         * Represents an intent to delete a collection.
         */
        data class DeleteCollection(val collection: String) : Intent
    }

    data class State(
        val collectionsList: CollectionsList = CollectionsList(),
        val selectedCollectionData: CollectionData? = null,
        val liveNodesData: List<String> = emptyList(),
        val mutating: Boolean = false,
        val selected: String? = null,
    )

    sealed interface Label {
        data class ReplicaAdded(val shard: String) : Label
        data class ReplicaDeleted(val shard: String, val replica: String) : Label
        data class CollectionDeleted(val name: String) : Label
        data class Error(val message: String) : Label
    }
}
