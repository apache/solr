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

package org.apache.solr.ui.components.auth.viewmodel

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
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.auth.data.HttpBasicAuthRepository
import org.apache.solr.ui.components.auth.domain.BasicAuthEvent
import org.apache.solr.ui.components.auth.domain.DefaultBasicAuthenticateUseCase
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.errors.InvalidCredentialsException

@OptIn(ExperimentalCoroutinesApi::class)
class BasicAuthStateHolderTest {

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
    fun `GIVEN valid credentials WHEN authenticate THEN Authenticated with BasicAuthMethod emitted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine), method = method)
        val events = collectEvents(stateHolder)

        stateHolder.changeUsername(validUsername)
        stateHolder.changePassword(validPassword)
        advanceUntilIdle()

        stateHolder.authenticate()
        advanceUntilIdle()

        assertContains(events, BasicAuthEvent.AuthenticationStarted)
        assertContains(events, BasicAuthEvent.Authenticated(method, validUsername, validPassword))
    }

    @Test
    fun `GIVEN invalid credentials WHEN authenticate THEN invalid credentials emitted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))
        val events = collectEvents(stateHolder)

        stateHolder.changeUsername("invalidUser")
        stateHolder.changePassword("someinvalid-password")
        advanceUntilIdle()

        stateHolder.authenticate()
        advanceUntilIdle()

        assertEquals(2, events.size, "Two events expected")
        assertContains(events, BasicAuthEvent.AuthenticationStarted)
        val outcome = events[1]
        assertIs<BasicAuthEvent.AuthenticationFailed>(outcome)
        assertIs<InvalidCredentialsException>(outcome.error)
    }

    @Test
    fun `GIVEN error state WHEN credentials change THEN error reset emitted`() = runTest {
        val engine = createMockEngine(authenticationResponseHandler)
        val stateHolder = createStateHolder(httpClient = HttpClient(engine))
        val events = collectEvents(stateHolder)

        stateHolder.changeUsername("invalidUser")
        stateHolder.changePassword("someinvalid-password")
        advanceUntilIdle()

        stateHolder.authenticate()
        advanceUntilIdle()

        assertContains(events, BasicAuthEvent.AuthenticationStarted)
        assertEquals(2, events.size, "Two events expected")
        assertIs<BasicAuthEvent.AuthenticationFailed>(events[1])

        stateHolder.changeUsername("newUsername")
        advanceUntilIdle()

        assertEquals(3, events.size, "Three events expected")
        assertContains(events, BasicAuthEvent.ErrorReset)
    }

    /**
     * Collects the events emitted by the [stateHolder] into the returned list.
     */
    private fun TestScope.collectEvents(stateHolder: BasicAuthStateHolder): List<BasicAuthEvent> {
        val events = mutableListOf<BasicAuthEvent>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            stateHolder.events.collect { events.add(it) }
        }
        return events
    }

    /**
     * Helper function for creating an instance of the [BasicAuthStateHolder].
     */
    private fun TestScope.createStateHolder(
        httpClient: HttpClient = HttpClient(),
        method: AuthMethod.BasicAuthMethod = AuthMethod.BasicAuthMethod(),
    ) = BasicAuthStateHolder(
        scope = this,
        method = method,
        basicAuthenticateUseCase = DefaultBasicAuthenticateUseCase(HttpBasicAuthRepository(httpClient)),
        dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
    )
}
