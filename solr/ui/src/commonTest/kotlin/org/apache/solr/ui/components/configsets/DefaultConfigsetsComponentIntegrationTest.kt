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

package org.apache.solr.ui.components.configsets

import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.essenty.lifecycle.resume
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.MockRequestHandler
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.apache.solr.ui.TestAppComponentContext
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.integration.DefaultConfigsetsComponent
import org.apache.solr.ui.components.start.integration.DefaultStartComponent
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.testHttpClient
import org.apache.solr.ui.utils.AppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultConfigsetsComponentIntegrationTest {

    private val configset1 = "_default"
    private val configset2 = "techproducts"
    private val configset3 = "getting_started"
    private val configSets = listOf(configset1, configset2, configset3)

    /**
     * Request handler that returns a list of configsets
     */
    private val configsetsResponseHandler: MockRequestHandler = { scope: MockRequestHandleScope, data: HttpRequestData ->
        val content = ListConfigsets(configSets)
        scope.respond(
            content = Json.encodeToString(value = content),
            status = HttpStatusCode.OK,
            headers = headersOf(
                name = HttpHeaders.ContentType,
                value = ContentType.Application.Json.toString(),
            ),
        )
    }

    @Test
    fun `GIVEN configsets WHEN initialized THEN configsets fetched`() = runTest {
        val engine = createMockEngine(configsetsResponseHandler)
        val component = createComponent(httpClient = testHttpClient(engine))
        advanceUntilIdle()

        component.model.value.configsets.forEach { configset ->
            assertContains(
                iterable = configSets,
                element = configset.name,
            )
        }
    }

    @Test
    fun `GIVEN configsets WHEN configsets fetched THEN configsets sorted`() = runTest {
        val engine = createMockEngine(configsetsResponseHandler)
        val component = createComponent(httpClient = testHttpClient(engine))
        advanceUntilIdle()

        assertContentEquals(
            expected = configSets.sorted(),
            actual = component.model.value.configsets.map(Configset::name),
        )
    }

    @Test
    fun `GIVEN configsets WHEN configset selected THEN selectedConfigset updated`() = runTest {
        val engine = createMockEngine(configsetsResponseHandler)
        val component = createComponent(httpClient = testHttpClient(engine))
        advanceUntilIdle()

        component.onSelectConfigset(name = configset2)
        advanceUntilIdle()

        assertEquals(
            expected = configset2,
            actual = component.model.value.selectedConfigset,
        )
    }

    /**
     * Helper function for creating an instance of the [DefaultStartComponent].
     */
    private fun TestScope.createComponent(
        componentContext: AppComponentContext = TestAppComponentContext(scheduler = testScheduler),
        storeFactory: StoreFactory = DefaultStoreFactory(),
        httpClient: HttpClient = HttpClient(),
    ): ConfigsetsComponent {
        val lifecycle = LifecycleRegistry()

        val component = DefaultConfigsetsComponent(
            componentContext = componentContext,
            storeFactory = storeFactory,
            httpClient = httpClient,
        )

        lifecycle.resume()
        return component
    }
}
