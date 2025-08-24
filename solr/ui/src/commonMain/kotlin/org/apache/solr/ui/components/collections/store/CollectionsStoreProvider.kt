package org.apache.solr.ui.components.collections.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.collections.data.ClusterStatusResponse
import org.apache.solr.ui.components.collections.data.CollectionInfo
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.data.toCollectionDataOrNull
import org.apache.solr.ui.components.collections.store.CollectionsStore.Intent
import org.apache.solr.ui.components.collections.store.CollectionsStore.State

internal class CollectionsStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {
    fun provide(): CollectionsStore = object :
        CollectionsStore,
        Store<Intent, State, Nothing> by storeFactory.create(
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
    }
    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, Nothing>(mainContext) {

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
                }
            }
        }
    }

    private object ReducerImpl :
        Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.CollectionsListUpdated -> copy(
                collectionsList = msg.collectionsList,
            )
            is Message.CollectionSelected -> {
                val data = msg.clusterStatusResponse.toCollectionDataOrNull(msg.name)
                println("Collection data: $data")
                copy(
                    selectedCollectionData = data,
                )
            }
        }
    }

    interface Client {
        suspend fun getCollections(): Result<CollectionsList>
        suspend fun getClusterStatus(collectionName: String): Result<ClusterStatusResponse>
    }
}
