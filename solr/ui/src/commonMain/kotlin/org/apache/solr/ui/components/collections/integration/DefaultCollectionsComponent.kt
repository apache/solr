package org.apache.solr.ui.components.collections.integration

import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import org.apache.solr.ui.components.collections.CollectionsComponent
import org.apache.solr.ui.components.collections.store.CollectionsStore
import org.apache.solr.ui.components.collections.store.CollectionsStoreProvider
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.environment.integration.HttpEnvironmentStoreClient
import org.apache.solr.ui.components.environment.integration.environmentStateToModel
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

    override fun selectCollection(name: String) {
        println("selectCollection: $name")
        store.accept(CollectionsStore.Intent.FetchCollectionData(name))
    }
}
