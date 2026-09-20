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

import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.http.Url
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.auth.domain.DefaultCreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.DefaultOAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.OAuthEvent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthorizationFlow
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.errors.UnauthorizedException

@OptIn(ExperimentalCoroutinesApi::class)
class OAuthStateHolderTest {

    private val method = AuthMethod.OAuthMethod(
        data = OAuthData(
            authorizationEndpoint = Url("https://auth.example.com/authorize"),
            clientId = "client-id",
            scope = "openid profile",
            authorizationFlow = AuthorizationFlow.CodePKCE,
            redirectUris = listOf(Url("http://127.0.0.1:8983/callback")),
            tokenEndpoint = Url("https://auth.example.com/token"),
        ),
    )

    @Test
    fun `WHEN authenticate THEN AuthenticationStarted is emitted`() = runTest {
        val stateHolder = createStateHolder {
            Result.failure(RuntimeException("noop"))
        }
        val events = collectEvents(stateHolder)

        stateHolder.authenticate()
        advanceUntilIdle()

        val event = assertIs<OAuthEvent.AuthenticationStarted>(
            value = events.firstOrNull(),
            message = "first event emitted should be AuthenticationStarted",
        )
        val url = event.url.toString()

        assertTrue(url.contains("client_id=client-id"))
        assertTrue(url.contains("response_type=code"))
        assertTrue(url.contains("code_challenge"))
        assertTrue(url.contains("state="))
    }

    @Test
    fun `WHEN unexpected error during authenticate THEN error state is set and emitted`() = runTest {
        val stateHolder = createStateHolder {
            // Fail immediately
            throw RuntimeException("oops")
        }
        val events = collectEvents(stateHolder)

        stateHolder.authenticate()
        advanceUntilIdle()

        val errorEvent = events.filterIsInstance<OAuthEvent.AuthenticationFailed>().single()
        assertIs<RuntimeException>(
            value = errorEvent.error,
            message = "error should be RuntimeException",
        )
        assertTrue(
            actual = stateHolder.uiState.value.hasError,
            message = "state should have error",
        )
    }

    @Test
    fun `WHEN authentication succeeds THEN Authenticated is emitted`() = runTest {
        val tokens = BearerTokens(
            accessToken = "access",
            refreshToken = "refresh",
        )

        val stateHolder = createStateHolder {
            Result.success(tokens)
        }
        val events = collectEvents(stateHolder)

        stateHolder.authenticate()
        advanceUntilIdle()

        val authenticated = events.filterIsInstance<OAuthEvent.Authenticated>().single()

        assertEquals("access", authenticated.accessToken)
        assertEquals("refresh", authenticated.refreshToken)
        assertEquals(method, authenticated.method)
    }

    @Test
    fun `WHEN authentication fails THEN AuthenticationFailed is emitted`() = runTest {
        val error = RuntimeException("boom")

        val stateHolder = createStateHolder {
            Result.failure(error)
        }
        val events = collectEvents(stateHolder)

        stateHolder.authenticate()
        advanceUntilIdle()

        val failedEvent = events.filterIsInstance<OAuthEvent.AuthenticationFailed>().single()
        assertEquals(error, failedEvent.error)

        assertTrue(stateHolder.uiState.value.hasError)
    }

    @Test
    fun `WHEN unauthorized THEN error is mapped to InvalidCredentialsException`() = runTest {
        val stateHolder = createStateHolder {
            Result.failure(exception = UnauthorizedException())
        }
        val events = collectEvents(stateHolder)

        stateHolder.authenticate()
        advanceUntilIdle()

        assertTrue(actual = events.size > 1, message = "multiple events should exist")
        val failed = events.filterIsInstance<OAuthEvent.AuthenticationFailed>().single()

        assertIs<InvalidCredentialsException>(
            value = failed.error,
            message = "error should be InvalidCredentialsException",
        )
        assertTrue(
            actual = stateHolder.uiState.value.hasError,
            message = "state should have error",
        )
    }

    /**
     * Collects the events emitted by the [stateHolder] into the returned list.
     */
    private fun TestScope.collectEvents(stateHolder: OAuthStateHolder): List<OAuthEvent> {
        val events = mutableListOf<OAuthEvent>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            stateHolder.events.collect { events.add(it) }
        }
        return events
    }

    private fun TestScope.createStateHolder(
        result: suspend () -> Result<BearerTokens>,
    ) = OAuthStateHolder(
        scope = this,
        method = method,
        createOAuthAuthorizationUseCase = DefaultCreateOAuthAuthorizationUseCase(),
        oAuthAuthenticateUseCase = DefaultOAuthAuthenticateUseCase(FakeOAuthRepository(result)),
        dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
    )
}
