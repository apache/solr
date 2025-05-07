package org.apache.solr.ui.components.auth.integration

import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import org.apache.solr.ui.components.auth.UnauthenticatedComponent
import org.apache.solr.ui.utils.AppComponentContext

class DefaultUnauthenticatedComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    private val output: (UnauthenticatedComponent.Output) -> Unit
) : UnauthenticatedComponent, AppComponentContext by componentContext {

    // TODO Implement me

    override fun onAbort() = output(UnauthenticatedComponent.Output.OnAbort)
}