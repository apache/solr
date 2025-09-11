package org.apache.solr.ui.components.collections.integration

import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import org.apache.solr.ui.components.collections.CollectionsComponent
import org.apache.solr.ui.components.collections.store.CollectionsStore
import org.apache.solr.ui.components.collections.store.CollectionsStoreProvider
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultCollectionsComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
) : CollectionsComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        CollectionsStoreProvider(
            storeFactory = storeFactory,
            client = HttpCollectionsStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, collectionsStateToModel)
    override val labels: Flow<CollectionsStore.Label> = store.labels

    override fun selectCollection(name: String) {
        store.accept(CollectionsStore.Intent.FetchCollectionData(name))
    }

    override fun fetchLiveNodesData() {
        store.accept(CollectionsStore.Intent.FetchLiveNodesData)
    }

    override fun addReplica(nodeName: String?, shardName: String, type: String) {
        println("addReplica: $nodeName, $shardName, $type")
        store.accept(CollectionsStore.Intent.AddReplica(nodeName, shardName, type))
    }

    override fun deleteReplica(shardName: String, replicaName: String) {
        store.accept(CollectionsStore.Intent.DeleteReplica(shardName, replicaName))
    }

    override fun deleteCollection(name: String) {
        store.accept(CollectionsStore.Intent.DeleteCollection(name))
    }

    override fun reloadCollection(name: String) {
        println("reloadCollection: $name")
        store.accept(CollectionsStore.Intent.Reload(name))
    }

    override fun fetchConfigSets() {
        store.accept(CollectionsStore.Intent.FetchConfigSet)
    }

    override fun createCollection(name: String, numShards: Int, replicas: Int, configSet: String) {
        store.accept(CollectionsStore.Intent.CreateCollection(name, numShards, replicas, configSet))
    }
}
