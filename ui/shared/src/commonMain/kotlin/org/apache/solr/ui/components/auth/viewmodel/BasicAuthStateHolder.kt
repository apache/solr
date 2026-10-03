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
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.auth.domain.BasicAuthEvent
import org.apache.solr.ui.components.auth.domain.BasicAuthenticateUseCase
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppDispatchers

/**
 * State holder that handles basic authentication with username and password.
 *
 * @property method The basic auth method the user is authenticating with.
 */
class BasicAuthStateHolder(
    private val scope: CoroutineScope,
    private val method: AuthMethod.BasicAuthMethod,
    private val basicAuthenticateUseCase: BasicAuthenticateUseCase,
    private val dispatchers: AppDispatchers,
) {

    /**
     * UI state of the basic authentication form.
     */
    val uiState: StateFlow<BasicAuthUiState>
        field = MutableStateFlow(BasicAuthUiState(realm = method.realm))

    /**
     * Events emitted by the state holder.
     */
    val events: SharedFlow<BasicAuthEvent>
        field = MutableSharedFlow<BasicAuthEvent>(extraBufferCapacity = 1)

    /**
     * Updates the username that is typed in.
     *
     * @param username The new username.
     */
    fun changeUsername(username: String) {
        resetError()
        uiState.update { it.copy(username = username, hasError = false) }
    }

    /**
     * Updates the password that is typed in.
     *
     * @param password The new password.
     */
    fun changePassword(password: String) {
        resetError()
        uiState.update { it.copy(password = password, hasError = false) }
    }

    /**
     * Tries to authenticate the user with the username and password that are currently typed in.
     */
    fun authenticate() {
        val username = uiState.value.username
        val password = uiState.value.password

        scope.launch {
            events.emit(BasicAuthEvent.AuthenticationStarted)

            withContext(dispatchers.io) {
                basicAuthenticateUseCase(username, password)
            }.onSuccess {
                // Authentication succeeded with the given credentials
                events.emit(BasicAuthEvent.Authenticated(method, username, password))
            }.onFailure { error ->
                uiState.update { it.copy(hasError = true) }
                events.emit(BasicAuthEvent.AuthenticationFailed(error))
            }
        }
    }

    /**
     * Notifies listeners about the reset of the error, if there is one.
     */
    private fun resetError() {
        if (uiState.value.hasError) {
            scope.launch { events.emit(BasicAuthEvent.ErrorReset) }
        }
    }
}

/**
 * UI state of the basic authentication form.
 *
 * @property realm The basic auth realm the user is supposed to provide the credentials for.
 * @property username The username that is currently typed in.
 * @property password The password that is currently typed in.
 * @property hasError Whether the current input has an error. This is a simplified variant that
 * does not distinguish input fields.
 */
data class BasicAuthUiState(
    val realm: String? = null,
    val username: String = "",
    val password: String = "",
    val hasError: Boolean = false,
)
