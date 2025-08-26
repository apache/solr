package org.apache.solr.ui.components.collections

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.data.ListConfigSets
import org.apache.solr.ui.components.collections.store.CollectionsStore

/**
 * Component interface that represents the collection section.
 */
interface CollectionsComponent {

    val model: StateFlow<Model>

    val labels: Flow<CollectionsStore.Label>

    data class Model(
        val collectionsList: CollectionsList = CollectionsList(),
        val selectedCollectionData: CollectionData? = null,
        val liveNodesData: List<String> = emptyList(),
        val mutating: Boolean = false,
        val configSets: ListConfigSets? = null,
    )

    fun selectCollection(name: String)
    fun fetchLiveNodesData()
    fun addReplica(nodeName: String?, shardName: String, type: String)
    fun deleteReplica(shardName: String, replicaName: String)
    fun deleteCollection(name: String)
    fun reloadCollection(name: String)
    fun fetchConfigSets()
    fun createCollection(name: String, numShards: Int, replicas: Int, configSet: String)
}
