package org.apache.solr.ui.components.collections.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.apache.solr.ui.components.collections.data.ClusterStatusResponse
import org.apache.solr.ui.components.collections.data.CollectionInfo
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.data.ZkTree
import org.apache.solr.ui.components.collections.data.liveNodes
import org.apache.solr.ui.components.collections.data.toCollectionDataOrNull
import org.apache.solr.ui.components.collections.store.CollectionsStore.Intent
import org.apache.solr.ui.components.collections.store.CollectionsStore.State
import org.apache.solr.ui.generated.resources.Res

internal class CollectionsStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {
    fun provide(): CollectionsStore = object :
        CollectionsStore,
        Store<Intent, State, CollectionsStore.Label> by storeFactory.create(
            name = "CollectionsStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(Action.FetchInitialCollectionsData),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Action {

        /**
         * Action used for initiating the initial fetch of environment data.
         */
        data object FetchInitialCollectionsData : Action
    }
    private sealed interface Message {
        data class CollectionsListUpdated(val collectionsList: CollectionsList) : Message
        data class CollectionSelected(val clusterStatusResponse: ClusterStatusResponse, val name: String) : Message
        data class LiveNodesUpdated(val zkTree: ZkTree) : Message
        data object MutationStarted : Message
        data object MutationFinished : Message
        data class SelectedChanged(val name: String) : Message
        data object SelectionCleared : Message
    }
    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, CollectionsStore.Label>(mainContext) {

        override fun executeAction(action: Action) = when (action) {
            Action.FetchInitialCollectionsData -> {
                fetchCollectionList()
            }
        }

        override fun executeIntent(intent: Intent) {
            when (intent) {
                is Intent.FetchCollectionList -> {
                    fetchCollectionList()
                }
                is Intent.FetchCollectionData -> {
                    fetchCollectionData(intent.name)
                }
                is Intent.FetchLiveNodesData -> {
                    fetchLiveNodesData()
                }
                is Intent.AddReplica -> {
                    val col = state().selected
                    if (col == null) {
                        publish(CollectionsStore.Label.Error("No collection selected"))
                        return
                    }
                    addReplica(col, intent.nodeName, intent.shardName, intent.type)
                }
                is Intent.DeleteReplica -> {
                    val col = state().selected
                    if (col == null) {
                        publish(CollectionsStore.Label.Error("No collection selected"))
                        return
                    }
                    deleteReplica(col, intent.shardName, intent.replicaName)
                }
                is Intent.DeleteCollection -> {
                    deleteCollection(intent.collection)
                }
            }
        }
        private fun fetchCollectionList() {
            scope.launch {
                // TODO Add coroutine exception handler
                withContext(ioContext) {
                    client.getCollections()
                }.onSuccess {
                    dispatch(Message.CollectionsListUpdated(it))
                }
                // TODO Add error handling
            }
        }
        private fun fetchCollectionData(name: String) {
            scope.launch {
                withContext(ioContext) {
                    client.getClusterStatus(name)
                }.onSuccess {
                    dispatch(Message.CollectionSelected(it, name))
                    dispatch(Message.SelectedChanged(name))
                }
            }
        }
        private fun fetchLiveNodesData() {
            scope.launch {
                withContext(ioContext) {
                    client.getZkTree()
                }.onSuccess {
                    dispatch(Message.LiveNodesUpdated(it))
                }
            }
        }
        private fun addReplica(collectionName: String, nodeName: String?, shardName: String, type: String) {
            dispatch(Message.MutationStarted)
            scope.launch {
                withContext(ioContext) {
                    client.addReplica(collectionName, nodeName, shardName, type)
                }.onSuccess {
                    publish(CollectionsStore.Label.ReplicaAdded(shardName))
                    fetchCollectionData(collectionName)
                    pollShardHealthyBackoff(collection = collectionName, shard = shardName)
                }.onFailure {
                    publish(CollectionsStore.Label.Error("Add replica failed"))
                }
            }
            dispatch(Message.MutationFinished)
        }

        private fun deleteReplica(collectionName: String, shardName: String, replicaName: String) {
            dispatch(Message.MutationStarted)
            scope.launch {
                withContext(ioContext) {
                    client.deleteReplica(collectionName, shardName, replicaName)
                }.onSuccess {
                    publish(CollectionsStore.Label.ReplicaDeleted(shardName, replicaName))
                    fetchCollectionData(collectionName)
                    pollShardHealthyBackoff(collection = collectionName, shard = shardName)
                }.onFailure {
                    publish(CollectionsStore.Label.Error("Delete replica failed"))
                }
            }
            dispatch(Message.MutationFinished)
        }

        private fun deleteCollection(name: String) {
            dispatch(Message.MutationStarted)
            scope.launch {
                withContext(ioContext) {
                    client.deleteCollection(name)
                }.onSuccess {
                    // if the deleted collection is currently selected, clear selection & details
                    if (state().selected == name) {
                        dispatch(Message.SelectionCleared)
                    }
                    publish(CollectionsStore.Label.CollectionDeleted(name))
                    fetchCollectionList()
                }.onFailure {
                    publish(CollectionsStore.Label.Error("Delete collection failed"))
                }
            }
            dispatch(Message.MutationFinished)
        }

        private suspend fun pollShardHealthyBackoff(
            collection: String,
            shard: String,
            maxAttempts: Int = 12,
            initialDelayMs: Long = 1000L,
            maxDelayMs: Long = 8000L,
        ) {
            var delayMs = initialDelayMs
            repeat(maxAttempts) {
                if (state().selected != collection) return // user navigated away
                delay(delayMs)
                fetchCollectionData(collection) // re-dispatches DetailsLoaded

                val s = state().selectedCollectionData?.shards[shard]
                val allActive = s?.replicas?.values?.all { it.state.equals("ACTIVE", true) } == true
                val hasActiveLeader = s?.replicas?.values?.any { it.leader.equals("true", true) && it.state.equals("ACTIVE", true) } == true
                if (allActive && hasActiveLeader) return // GREEN enough

                delayMs = (delayMs * 2).coerceAtMost(maxDelayMs) // backoff
            }
            // still not green → let UI show “Recovering…”; user can press Reload
        }
    }

    private object ReducerImpl :
        Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.CollectionsListUpdated ->
                copy(collectionsList = msg.collectionsList)
            is Message.CollectionSelected -> {
                val data = msg.clusterStatusResponse.toCollectionDataOrNull(msg.name)
                copy(selectedCollectionData = data)
            }
            is Message.LiveNodesUpdated -> {
                val nodes = msg.zkTree.liveNodes()
                copy(liveNodesData = nodes)
            }
            is Message.MutationStarted -> {
                copy(mutating = true)
            }
            is Message.MutationFinished -> {
                copy(mutating = false)
            }
            is Message.SelectedChanged -> {
                copy(selected = msg.name)
            }
            is Message.SelectionCleared -> {
                copy(
                    selected = null,
                    selectedCollectionData = null,
                )
            }
        }
    }

    interface Client {
        suspend fun getCollections(): Result<CollectionsList>
        suspend fun getClusterStatus(collectionName: String): Result<ClusterStatusResponse>
        suspend fun getZkTree(): Result<ZkTree>
        suspend fun addReplica(collectionName: String, nodeName: String?, shardName: String, type: String): Result<JsonObject>
        suspend fun deleteReplica(collectionName: String, shardName: String, replicaName: String): Result<JsonObject>
        suspend fun deleteCollection(collectionName: String): Result<JsonObject>
    }
}
