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

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.http.ParametersBuilder
import io.ktor.http.URLBuilder
import io.ktor.utils.io.CancellationException
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.auth.generateCodeChallenge
import org.apache.solr.ui.components.auth.generateCodeVerifier
import org.apache.solr.ui.components.auth.generateOAuthState
import org.apache.solr.ui.components.auth.getRedirectUri
import org.apache.solr.ui.components.auth.store.OAuthStore.Intent
import org.apache.solr.ui.components.auth.store.OAuthStore.Label
import org.apache.solr.ui.components.auth.store.OAuthStore.State
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.utils.parseError

class OAuthStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val method: AuthMethod.OAuthMethod,
) {

    fun provide(): OAuthStore = object :
        OAuthStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "OAuthStore",
            initialState = State(method = method),
            bootstrapper = SimpleBootstrapper(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {
        /**
         * Message that is dispatched when an authentication error occurred.
         */
        data class AuthenticationFailed(val error: Throwable) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {

        private var authJob: Job? = null

        override fun executeIntent(intent: Intent) {
            when (intent) {
                is Intent.Authenticate -> authenticate()
            }
        }

        private fun authenticate(): Unit = with(state()) {
            val verifier = generateCodeVerifier()
            val challenge = generateCodeChallenge(verifier)
            val oAuthState = generateOAuthState()
            val authorizeUrl = URLBuilder(method.data.authorizationEndpoint)
                .apply {
                    encodedParameters = ParametersBuilder().apply {
                        set("client_id", method.data.clientId)
                        set("scope", method.data.scope)
                        set("redirect_uri", getRedirectUri())
                        set("response_type", "code")
                        set("code_challenge_method", "S256")
                        set("code_challenge", challenge)
                        set("state", oAuthState)
                    }
                }
                .build()

            authJob?.cancel()
            authJob = scope.launch {
                publish(Label.AuthenticationStarted(url = authorizeUrl))
                try {
                    withContext(ioContext) {
                        client.authenticate(oAuthState, verifier, method.data)
                    }.onSuccess {
                        // Authentication succeeded with the given credentials
                        publish(
                            Label.Authenticated(
                                method = method,
                                accessToken = it.accessToken,
                                refreshToken = it.refreshToken,
                            ),
                        )
                    }.onFailure { error ->
                        handleConnectionError(error)
                    }
                } catch (error: CancellationException) {
                    throw error
                } catch (throwable: Throwable) {
                    val parsed = parseError(throwable)
                    publish(Label.AuthenticationFailed(error = parsed))
                    dispatch(Message.AuthenticationFailed(error = parsed))
                }
            }
        }

        private fun handleConnectionError(error: Throwable) {
            val mappedError: Throwable = when (error) {
                // Unauthorized response means that the credentials are invalid
                is UnauthorizedException -> InvalidCredentialsException()
                else -> error
            }

            dispatch(Message.AuthenticationFailed(mappedError))
            publish(Label.AuthenticationFailed(mappedError))
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.AuthenticationFailed -> copy(error = msg.error)
        }
    }

    interface Client {

        /**
         * Authenticates the user with the current Solr instance.
         *
         * @param state The state value used in the authorization flow with PKCE.
         * @param verifier Code verifier used in the authorization flow with PKCE.
         * @param data The OAuth data to use for the auth flow.
         * @return Returns success results iff the user has successfully authenticated.
         */
        suspend fun authenticate(state: String, verifier: String, data: OAuthData): Result<BearerTokens>
    }
}
