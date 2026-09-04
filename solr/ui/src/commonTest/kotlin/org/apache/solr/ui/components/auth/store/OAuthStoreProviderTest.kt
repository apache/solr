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

package org.apache.solr.ui.components.auth.store

import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.http.Url
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.auth.store.OAuthStore.Intent
import org.apache.solr.ui.components.auth.store.OAuthStore.Label
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthorizationFlow
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.errors.UnauthorizedException

@OptIn(ExperimentalCoroutinesApi::class)
class OAuthStoreProviderTest {

    private lateinit var client: FakeOAuthClient
    private lateinit var store: OAuthStore

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

    private fun createStore(
        testScheduler: TestCoroutineScheduler,
        result: suspend () -> Result<BearerTokens>,
    ) {
        client = FakeOAuthClient(result)

        store = OAuthStoreProvider(
            storeFactory = DefaultStoreFactory(),
            client = client,
            mainContext = UnconfinedTestDispatcher(testScheduler),
            ioContext = StandardTestDispatcher(testScheduler),
            method = method,
        ).provide()
        store.init()
    }

    @Test
    fun `WHEN authenticate THEN AuthenticationStarted is published`() = runTest {
        createStore(testScheduler) {
            Result.failure(RuntimeException("noop"))
        }

        val labels = mutableListOf<Label>()
        store.labels.onEach(labels::add).launchIn(backgroundScope)
        runCurrent()

        store.accept(Intent.Authenticate)
        runCurrent()
        advanceUntilIdle()

        val label = assertIs<Label.AuthenticationStarted>(
            value = labels.firstOrNull(),
            message = "first label emitted should be AuthenticationStarted",
        )
        val url = label.url.toString()

        assertTrue(url.contains("client_id=client-id"))
        assertTrue(url.contains("response_type=code"))
        assertTrue(url.contains("code_challenge"))
        assertTrue(url.contains("state="))
    }

    @Test
    fun `WHEN unexpected error during authenticate THEN error state is set and published`() = runTest {
        createStore(testScheduler) {
            // Fail immediately
            throw RuntimeException("oops")
        }

        val labels = mutableListOf<Label>()
        store.labels.onEach(labels::add).launchIn(backgroundScope)
        runCurrent()

        store.accept(Intent.Authenticate)
        runCurrent()
        advanceUntilIdle()

        val errorLabel = assertIs<Label.AuthenticationFailed>(
            value = labels.filterIsInstance<Label.AuthenticationFailed>().single(),
            message = "first label emitted should be AuthenticationStarted",
        )
        assertIs<RuntimeException>(
            value = errorLabel.error,
            message = "error should be RuntimeException",
        )
        assertNotNull(
            actual = store.state.error,
            message = "state should have error",
        )
    }

    @Test
    fun `WHEN authentication succeeds THEN Authenticated is published`() = runTest {
        val tokens = BearerTokens(
            accessToken = "access",
            refreshToken = "refresh",
        )

        createStore(testScheduler) {
            Result.success(tokens)
        }

        val labels = mutableListOf<Label>()
        store.labels.onEach(labels::add).launchIn(backgroundScope)
        runCurrent()

        store.accept(Intent.Authenticate)
        runCurrent()
        advanceUntilIdle()

        val authenticated = labels.filterIsInstance<Label.Authenticated>().single()

        assertEquals("access", authenticated.accessToken)
        assertEquals("refresh", authenticated.refreshToken)
        assertEquals(method, authenticated.method)
    }

    @Test
    fun `WHEN authentication fails THEN AuthenticationFailed is published`() = runTest {
        val error = RuntimeException("boom")

        createStore(testScheduler) {
            Result.failure(error)
        }

        val labels = mutableListOf<Label>()
        store.labels.onEach(labels::add).launchIn(backgroundScope)
        runCurrent()

        store.accept(Intent.Authenticate)
        runCurrent()
        advanceUntilIdle()

        val failedLabel = labels.filterIsInstance<Label.AuthenticationFailed>().single()
        assertEquals(error, failedLabel.error)

        assertEquals(error, store.state.error)
    }

    @Test
    fun `WHEN unauthorized THEN error is mapped to InvalidCredentialsException`() = runTest {
        createStore(testScheduler) {
            Result.failure(exception = UnauthorizedException())
        }

        val labels = mutableListOf<Label>()
        store.labels.onEach(labels::add).launchIn(backgroundScope)
        runCurrent()

        store.accept(Intent.Authenticate)
        runCurrent()
        advanceUntilIdle()

        assertTrue(actual = labels.size > 1, message = "multiple labels should exist")
        val failed = labels.filterIsInstance<Label.AuthenticationFailed>().single()

        assertIs<InvalidCredentialsException>(
            value = failed.error,
            message = "error should be InvalidCredentialsException",
        )
        assertIs<InvalidCredentialsException>(
            value = store.state.error,
            message = "state error should be InvalidCredentialsException",
        )
    }
}
