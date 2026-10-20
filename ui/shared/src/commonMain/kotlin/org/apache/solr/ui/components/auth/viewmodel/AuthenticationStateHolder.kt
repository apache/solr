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

import io.ktor.http.Url
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.apache.solr.ui.components.auth.domain.AuthenticationEvent
import org.apache.solr.ui.components.auth.domain.BasicAuthEvent
import org.apache.solr.ui.components.auth.domain.BasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.CreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.OAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.OAuthEvent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.error_invalid_credentials
import org.apache.solr.ui.shared.generated.resources.error_unknown
import org.apache.solr.ui.utils.AppDispatchers
import org.jetbrains.compose.resources.StringResource

/**
 * State holder of the authentication screen that takes care of the authentication processes.
 *
 * The main role of this state holder is to hold and manage a shared authentication state that can
 * be used across all authentication options available to the user. This state can contain a shared
 * error state, an "authenticating" state and various other data that need to be in sync if multiple
 * authentication options are available at the same time.
 *
 * Note that Solr does not support multiple authentication methods of the same type with the
 * MultiAuthPlugin, so there will be always up to one state holder per method ([basicAuth] and
 * [oAuth]), and they exist only if the respective method is supported.
 *
 * @property url The URL of the Solr instance the user is authenticating against.
 * @param methods The authentication methods that are supported by the Solr instance.
 */
class AuthenticationStateHolder(
    private val scope: CoroutineScope,
    private val url: Url,
    methods: List<AuthMethod>,
    basicAuthenticateUseCase: BasicAuthenticateUseCase,
    createOAuthAuthorizationUseCase: CreateOAuthAuthorizationUseCase,
    oAuthAuthenticateUseCase: OAuthAuthenticateUseCase,
    dispatchers: AppDispatchers,
) {

    /**
     * State holder for authentication with credentials (basic auth), if it is supported.
     */
    val basicAuth: BasicAuthStateHolder? = methods
        .filterIsInstance<AuthMethod.BasicAuthMethod>()
        .lastOrNull()
        ?.let { method ->
            BasicAuthStateHolder(
                scope = scope,
                method = method,
                basicAuthenticateUseCase = basicAuthenticateUseCase,
                dispatchers = dispatchers,
            )
        }

    /**
     * State holder for authentication with OAuth (bearer token), if it is supported.
     */
    val oAuth: OAuthStateHolder? = methods
        .filterIsInstance<AuthMethod.OAuthMethod>()
        .lastOrNull()
        ?.let { method ->
            OAuthStateHolder(
                scope = scope,
                method = method,
                createOAuthAuthorizationUseCase = createOAuthAuthorizationUseCase,
                oAuthAuthenticateUseCase = oAuthAuthenticateUseCase,
                dispatchers = dispatchers,
            )
        }

    /**
     * UI state of the authentication screen.
     */
    val uiState: StateFlow<AuthenticationUiState>
        field = MutableStateFlow(AuthenticationUiState(url = url.toString(), methods = methods))

    /**
     * Events emitted by the state holder that are meant to be handled by the parent.
     */
    val events: SharedFlow<AuthenticationEvent>
        field = MutableSharedFlow<AuthenticationEvent>(extraBufferCapacity = 1)

    /**
     * URLs of the identity provider that are supposed to be opened for authorizing the user.
     */
    val authorizationUrls: SharedFlow<Url>
        field = MutableSharedFlow<Url>(extraBufferCapacity = 1)

    init {
        // Undispatched start makes sure the collectors are subscribed before anyone can emit
        basicAuth?.let { holder ->
            scope.launch(start = CoroutineStart.UNDISPATCHED) {
                holder.events.collect(::handleBasicAuthEvent)
            }
        }
        oAuth?.let { holder ->
            scope.launch(start = CoroutineStart.UNDISPATCHED) {
                holder.events.collect(::handleOAuthEvent)
            }
        }
    }

    /**
     * Aborts the authentication attempt.
     */
    fun abort() {
        scope.launch { events.emit(AuthenticationEvent.Aborted) }
    }

    private suspend fun handleBasicAuthEvent(event: BasicAuthEvent) = when (event) {
        is BasicAuthEvent.AuthenticationStarted -> startAuthenticating()

        is BasicAuthEvent.Authenticated -> events.emit(
            AuthenticationEvent.Authenticated(
                option = AuthOption.BasicAuthOption(
                    url = url,
                    username = event.username,
                    password = event.password,
                    realm = event.method.realm,
                ),
            ),
        )

        is BasicAuthEvent.AuthenticationFailed -> failAuthentication(event.error)

        is BasicAuthEvent.ErrorReset -> resetError()
    }

    private suspend fun handleOAuthEvent(event: OAuthEvent) = when (event) {
        is OAuthEvent.AuthenticationStarted -> {
            startAuthenticating()
            authorizationUrls.emit(event.url)
        }

        is OAuthEvent.Authenticated -> events.emit(
            AuthenticationEvent.Authenticated(
                option = AuthOption.OAuthOption(
                    url = url,
                    accessToken = event.accessToken,
                    refreshToken = event.refreshToken,
                    realm = event.method.realm,
                ),
            ),
        )

        is OAuthEvent.AuthenticationFailed -> failAuthentication(event.error)
    }

    private fun startAuthenticating() = uiState.update { it.copy(isAuthenticating = true) }

    private fun failAuthentication(error: Throwable) = uiState.update {
        it.copy(error = error.toErrorResource(), isAuthenticating = false)
    }

    private fun resetError() = uiState.update { it.copy(error = null) }
}

/**
 * UI state of the authentication screen.
 *
 * @property url URL of the Solr instance the user is trying to connect.
 * @property methods List of authentication methods to render.
 * @property isAuthenticating Whether a connection is currently established.
 * @property error The error that may have occurred.
 */
data class AuthenticationUiState(
    val url: String = "",
    val methods: List<AuthMethod> = emptyList(),
    val isAuthenticating: Boolean = false,
    val error: StringResource? = null,
)

private fun Throwable.toErrorResource(): StringResource = when (this) {
    is InvalidCredentialsException -> Res.string.error_invalid_credentials
    else -> Res.string.error_unknown
}
