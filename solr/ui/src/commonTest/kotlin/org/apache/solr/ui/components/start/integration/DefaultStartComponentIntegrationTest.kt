package org.apache.solr.ui.components.start.integration

import com.arkivanov.decompose.DefaultComponentContext
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.essenty.lifecycle.resume
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.client.HttpClient
import kotlin.test.Ignore
import kotlin.test.Test
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.StartComponent.Output
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.DefaultAppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultStartComponentIntegrationTest {

    @Test
    @Ignore
    fun `GIVEN initial state WHEN onConnect THEN use default Solr URL`() = runTest {
        val outputStack = mutableListOf<Output>()
        val component = createComponent(
            output = { outputStack.add(it) }
        )
        TODO("Not implemented yet")
    }

    @Test
    @Ignore
    fun `GIVEN invalid URL WHEN onConnect THEN invalidUrlError`() {
        TODO("Not implemented yet")
    }

    @Test
    @Ignore
    fun `GIVEN valid Solr URL WHEN onConnect THEN connection request sent`() {
        TODO("Not implemented yet")
    }

    private fun CoroutineScope.createComponent(
        lifecycle: LifecycleRegistry = LifecycleRegistry(),
        componentContext: AppComponentContext = DefaultAppComponentContext(
            componentContext = DefaultComponentContext(lifecycle = lifecycle),
            mainContext = coroutineContext + StandardTestDispatcher(),
            ioContext = coroutineContext + UnconfinedTestDispatcher(),
        ),
        storeFactory: StoreFactory = DefaultStoreFactory(),
        httpClient: HttpClient = HttpClient(),
        output: (Output) -> Unit = {},
    ): StartComponent {
        val lifecycle = LifecycleRegistry()

        val component =
            DefaultStartComponent(
                componentContext = componentContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                output = output,
            )

        lifecycle.resume()
        return component
    }
}