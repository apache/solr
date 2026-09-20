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

package org.apache.solr.ui.components.start.viewmodel

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.MockRequestHandler
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpStatusCode
import io.ktor.http.URLBuilder
import io.ktor.http.fullPath
import io.ktor.http.path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.start.data.HttpStartRepository
import org.apache.solr.ui.components.start.domain.DefaultConnectUseCase
import org.apache.solr.ui.components.start.domain.StartEvent
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.error_invalid_url

@OptIn(ExperimentalCoroutinesApi::class)
class StartStateHolderIntegrationTest {

    /**
     * Response handler that always responds with HTTP code OK.
     */
    private val okResponseHandler: MockRequestHandler = { scope: MockRequestHandleScope, data: HttpRequestData ->
        scope.respond(content = "Ignore", status = HttpStatusCode.OK)
    }

    @Test
    fun `GIVEN initial state WHEN connect THEN use default Solr URL`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))

        stateHolder.connect()
        advanceUntilIdle()

        assertEquals(
            expected = 1,
            actual = engine.requestHistory.size,
            message = "Expected one request in history",
        )
        assertEquals(
            // As of now connection is established by calling this endpoint
            // Note that the default host is the window.location.url (localhost:9876) on wasmJs
            // and 127.0.0.1:8983 on JVM
            expected = "/api/node/system",
            actual = engine.requestHistory[0].url.fullPath,
        )
    }

    @Test
    fun `GIVEN invalid URL WHEN connect THEN invalidUrlError`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))

        stateHolder.changeSolrUrl("some.-invalid-url")

        stateHolder.connect()
        advanceUntilIdle()

        assertEquals(
            expected = 0,
            actual = engine.requestHistory.size,
            message = "Expected no request in history",
        )

        assertEquals(
            expected = Res.string.error_invalid_url,
            actual = stateHolder.uiState.value.error,
            message = "Expected invalid url error",
        )
    }

    @Test
    fun `GIVEN valid Solr URL WHEN connect THEN connection request sent`() = runTest {
        val engine = createMockEngine(okResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))
        val validSolrUrl = "https://my-solr-instance.local/"

        stateHolder.changeSolrUrl(validSolrUrl)
        stateHolder.connect()
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
    fun `GIVEN a solr instance with no auth WHEN connect THEN event Connected`() = runTest {
        val events = mutableListOf<StartEvent>()
        val engine = createMockEngine(okResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            stateHolder.events.collect { events.add(it) }
        }

        stateHolder.connect()
        advanceUntilIdle()

        assertEquals(
            expected = 1,
            actual = events.size,
            message = "Expected one event",
        )
        assertIs<StartEvent.Connected>(
            value = events[0],
            message = "Expected event to be Connected",
        )
    }

    @Test
    fun `GIVEN input error WHEN input changes THEN error resets`() = runTest {
        val stateHolder = createStateHolder()
        stateHolder.changeSolrUrl("some.-invalid-url")
        // Cause an error in state
        stateHolder.connect()

        advanceUntilIdle()
        assertNotNull(stateHolder.uiState.value.error)

        stateHolder.changeSolrUrl("some-other-url")
        advanceUntilIdle()

        assertNull(stateHolder.uiState.value.error)
    }

    /**
     * Helper function for creating an instance of the [StartStateHolder].
     */
    private fun TestScope.createStateHolder(
        httpClient: HttpClient = HttpClient(),
    ) = StartStateHolder(
        scope = this,
        connectUseCase = DefaultConnectUseCase(HttpStartRepository(httpClient)),
        dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
    )
}
