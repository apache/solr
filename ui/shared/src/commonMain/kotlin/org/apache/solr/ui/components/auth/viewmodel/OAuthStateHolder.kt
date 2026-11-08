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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.auth.domain.CreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.OAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.OAuthEvent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppDispatchers

/**
 * State holder that handles authentication with OAuth via an identity provider.
 *
 * @property method The OAuth method the user is authenticating with.
 */
class OAuthStateHolder(
    private val scope: CoroutineScope,
    private val method: AuthMethod.OAuthMethod,
    private val createOAuthAuthorizationUseCase: CreateOAuthAuthorizationUseCase,
    private val oAuthAuthenticateUseCase: OAuthAuthenticateUseCase,
    private val dispatchers: AppDispatchers,
) {

    private var authJob: Job? = null

    /**
     * UI state of the OAuth authentication.
     */
    val uiState: StateFlow<OAuthUiState>
        field = MutableStateFlow(OAuthUiState(realm = method.realm))

    /**
     * Events emitted by the state holder.
     */
    val events: SharedFlow<OAuthEvent>
        field = MutableSharedFlow<OAuthEvent>(extraBufferCapacity = 1)

    /**
     * Starts an authentication attempt with the OAuth method.
     */
    fun authenticate() {
        val authorization = createOAuthAuthorizationUseCase(method)

        authJob?.cancel()
        authJob = scope.launch {
            events.emit(OAuthEvent.AuthenticationStarted(url = authorization.url))

            withContext(dispatchers.io) {
                oAuthAuthenticateUseCase(authorization, method)
            }.onSuccess {
                // Authentication succeeded with the given credentials
                events.emit(
                    OAuthEvent.Authenticated(
                        method = method,
                        accessToken = it.accessToken,
                        refreshToken = it.refreshToken,
                    ),
                )
            }.onFailure { error ->
                uiState.update { it.copy(hasError = true) }
                events.emit(OAuthEvent.AuthenticationFailed(error))
            }
        }
    }
}

/**
 * UI state of the OAuth authentication.
 *
 * @property realm The OAuth realm the user is supposed to authenticate against.
 * @property hasError Whether the current input has an error.
 */
data class OAuthUiState(
    val realm: String? = null,
    val hasError: Boolean = false,
)
