/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.ui.components.start.integration

import com.arkivanov.decompose.DefaultComponentContext
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.essenty.lifecycle.resume
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.core.utils.isAssertOnMainThreadEnabled
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.MockRequestHandler
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpStatusCode
import io.ktor.http.URLBuilder
import io.ktor.http.path
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.StartComponent.Output
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.error_invalid_url
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.DEFAULT_SOLR_URL
import org.apache.solr.ui.utils.DefaultAppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultStartComponentIntegrationTest {

    /**
     * Response handler that always responds with HTTP code OK.
     */
    private val okResponseHandler: MockRequestHandler = { scope: MockRequestHandleScope, data: HttpRequestData ->
        scope.respond(content = "Ignore", status = HttpStatusCode.OK)
    }

    /**
     * Response handler that always responds with HTTP code Forbidden.
     */
    private val forbiddenResponseHandler: MockRequestHandler = { scope: MockRequestHandleScope, data: HttpRequestData ->
        scope.respond(content = "Forbidden", status = HttpStatusCode.Forbidden)
    }

    @BeforeTest
    fun beforeTest() {
        isAssertOnMainThreadEnabled = false
    }

    @AfterTest
    fun afterTest() {
        isAssertOnMainThreadEnabled = true
    }

    @Test
    fun `GIVEN initial state WHEN onConnect THEN use default Solr URL`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val component = createComponent(httpClient = HttpClient(engine))

        component.onConnect()
        advanceUntilIdle()

        assertEquals(
            expected = 1,
            actual = engine.requestHistory.size,
            message = "Expected one request in history",
        )
        assertEquals(
            expected = URLBuilder(DEFAULT_SOLR_URL).apply {
                // As of now connection is established by calling this endpoint
                path("api/node/system")
            }.build(),
            actual = engine.requestHistory[0].url,
        )
    }

    @Test
    fun `GIVEN invalid URL WHEN onConnect THEN invalidUrlError`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val component = createComponent(httpClient = HttpClient(engine))

        component.onSolrUrlChange("some.-invalid-url")

        component.onConnect()
        advanceUntilIdle()

        assertEquals(
            expected = 0,
            actual = engine.requestHistory.size,
            message = "Expected no request in history",
        )

        assertEquals(
            expected = Res.string.error_invalid_url,
            actual = component.model.value.error,
            message = "Expected invalid url error",
        )
    }

    @Test
    fun `GIVEN valid Solr URL WHEN onConnect THEN connection request sent`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val component = createComponent(httpClient = HttpClient(engine))
        val validSolrUrl = "https://my-solr-instance.local/"

        component.onSolrUrlChange(validSolrUrl)
        component.onConnect()
        advanceUntilIdle()

        assertEquals(
            expected = 1,
            actual = engine.requestHistory.size,
            message = "Expected one request in history",
        )
        assertEquals(
            expected = URLBuilder(validSolrUrl).apply {
                // As of now connection is established by calling this endpoint
                path("api/node/system")
            }.build(),
            actual = engine.requestHistory[0].url,
        )
    }

    @Test
    fun `GIVEN a solr instance with no auth WHEN onConnect THEN output Connected`() = runTest {
        val outputStack = mutableListOf<Output>()
        val engine = createMockEngine(okResponseHandler)
        val component = createComponent(
            output = { outputStack.add(it) },
            httpClient = HttpClient(engine),
        )

        component.onConnect()
        advanceUntilIdle()

        assertEquals(
            expected = 1,
            actual = outputStack.size,
            message = "Expected one output",
        )
        assertIs<Output.OnConnected>(
            value = outputStack[0],
            message = "Expected output to be Connected",
        )
    }

    @Test
    fun `GIVEN input error WHEN input changes THEN error resets`() = runTest {
        val component = createComponent()
        component.onSolrUrlChange("some.-invalid-url")
        // Cause an error in state
        component.onConnect()

        advanceUntilIdle()
        assertNotNull(component.model.value.error)

        component.onSolrUrlChange("some-other-url")
        advanceUntilIdle()

        assertNull(component.model.value.error)
    }

    /**
     * Helper function for creating an instance of the [DefaultStartComponent].
     */
    private fun TestScope.createComponent(
        lifecycle: LifecycleRegistry = LifecycleRegistry(),
        scheduler: TestCoroutineScheduler = testScheduler,
        componentContext: AppComponentContext = DefaultAppComponentContext(
            componentContext = DefaultComponentContext(lifecycle = lifecycle),
            mainContext = StandardTestDispatcher(scheduler),
            ioContext = UnconfinedTestDispatcher(scheduler),
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
