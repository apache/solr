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
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.MockRequestHandler
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpStatusCode
import io.ktor.utils.io.core.toByteArray
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.BasicAuthComponent.Output
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.DefaultAppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultBasicAuthComponentTest {

    /**
     * A normal basic auth method with the realm "solr".
     */
    private val method = AuthMethod.BasicAuthMethod(realm = "solr")

    /**
     * Valid username used for authentication.
     */
    private val validUsername = "Player1"

    /**
     * Valid password used for authentication.
     */
    private val validPassword = "SomeSuperSecurePassword12345"

    /**
     * The authorization header that should be used for basic auth with [validUsername] and
     * [validPassword].
     */
    @OptIn(ExperimentalEncodingApi::class)
    private val validBasicAuthHeader =
        "Basic ${Base64.encode("$validUsername:$validPassword".toByteArray())}"

    private val authenticationResponseHandler: MockRequestHandler = { scope: MockRequestHandleScope, data: HttpRequestData ->
        if (data.headers["Authorization"] == validBasicAuthHeader) {
            scope.respond(content = "ok", status = HttpStatusCode.OK)
        } else {
            scope.respond(content = "invalid credentials", status = HttpStatusCode.Unauthorized)
        }
    }

    @Test
    fun `GIVEN valid credentials WHEN authenticate THEN Authenticated with BasicAuthMethod outputted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val outputs = mutableListOf<Output>()

        val component = createComponent(
            httpClient = HttpClient(engine),
            output = { outputs.add(it) },
            method = method,
        )
        component.onChangeUsername(validUsername)
        component.onChangePassword(validPassword)
        advanceUntilIdle()

        component.onAuthenticate()
        advanceUntilIdle()

        assertContains(outputs, Output.Authenticating)
        assertContains(outputs, Output.Authenticated(method, validUsername, validPassword))
    }

    @Test
    fun `GIVEN invalid credentials WHEN authenticate THEN invalid credentials outputted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val outputs = mutableListOf<Output>()

        val component = createComponent(
            httpClient = HttpClient(engine),
            output = { outputs.add(it) },
        )
        component.onChangeUsername("invalidUser")
        component.onChangePassword("someinvalid-password")
        advanceUntilIdle()

        component.onAuthenticate()
        advanceUntilIdle()

        assertEquals(2, outputs.size, "Two outputs expected")
        assertContains(outputs, Output.Authenticating)
        val outcome = outputs[1]
        assertIs<Output.AuthenticationFailed>(outcome)
        assertIs<InvalidCredentialsException>(outcome.error)
    }

    @Test
    fun `GIVEN error state WHEN credentials change THEN error reset outputted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val outputs = mutableListOf<Output>()

        val component = createComponent(
            httpClient = HttpClient(engine),
            output = { outputs.add(it) },
        )
        component.onChangeUsername("invalidUser")
        component.onChangePassword("someinvalid-password")
        advanceUntilIdle()

        component.onAuthenticate()
        advanceUntilIdle()

        assertContains(outputs, Output.Authenticating)
        assertEquals(2, outputs.size, "Two outputs expected")
        assertIs<Output.AuthenticationFailed>(outputs[1])

        component.onChangeUsername("newUsername")
        advanceUntilIdle()

        assertEquals(3, outputs.size, "Three outputs expected")
        assertContains(outputs, Output.ErrorReset)
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
        method: AuthMethod.BasicAuthMethod = AuthMethod.BasicAuthMethod(),
        output: (Output) -> Unit = {},
    ): BasicAuthComponent {
        val lifecycle = LifecycleRegistry()

        val component =
            DefaultBasicAuthComponent(
                componentContext = componentContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                method = method,
                output = output,
            )

        lifecycle.resume()
        return component
    }
}
