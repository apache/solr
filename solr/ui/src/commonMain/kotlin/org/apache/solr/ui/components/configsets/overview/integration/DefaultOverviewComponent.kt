package org.apache.solr.ui.components.configsets.overview.integration

import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import org.apache.solr.ui.components.configsets.overview.OverviewComponent
import org.apache.solr.ui.utils.AppComponentContext

class DefaultOverviewComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
) : OverviewComponent, AppComponentContext by componentContext {
    
}
