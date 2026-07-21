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

package org.apache.solr.ui.components.auth.integration

import com.arkivanov.decompose.DefaultComponentContext
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.essenty.lifecycle.resume
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.client.HttpClient
import io.ktor.http.Url
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent.Output
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.DefaultAppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultAuthenticationComponentIntegrationTest {

    @Test
    fun `GIVEN basic auth method THEN basicAuthSlot populated`() = runTest {
        val component = createComponent(methods = listOf(AuthMethod.BasicAuthMethod(realm = "solr")))
        advanceUntilIdle()

        assertNotNull(component.basicAuthSlot.value)
    }

    /**
     * Helper function for creating an instance of the [DefaultAuthenticationComponent].
     */
    @OptIn(ExperimentalCoroutinesApi::class)
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
        urlString: String = "",
        methods: List<AuthMethod> = emptyList(),
        output: (Output) -> Unit = {},
    ): AuthenticationComponent {
        val lifecycle = LifecycleRegistry()

        val component =
            DefaultAuthenticationComponent(
                componentContext = componentContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                output = output,
                url = Url(urlString),
                methods = methods,
            )

        lifecycle.resume()
        return component
    }
}
