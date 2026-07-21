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

package org.apache.solr.ui.components.auth

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.auth.store.OAuthStore.Label
import org.apache.solr.ui.domain.AuthMethod

/**
 * Component interface that is handling basic authentication with username and password.
 */
interface OAuthComponent {

    val model: StateFlow<Model>

    val labels: Flow<Label>

    /**
     * Tries to authenticate the user with the given authorization URL.
     */
    fun onAuthenticate()

    /**
     * Data class model that represents the [OAuthComponent] state.
     *
     * @property realm The OAuth realm the user is supposed to authenticate against.
     * @property hasError Whether the current input has an error.
     */
    data class Model(
        val realm: String? = null,
        val hasError: Boolean = false,
    )

    sealed interface Output {

        /**
         * Output that is emitted when a connection process is started.
         */
        data object Authenticating : Output

        /**
         * Output that is emitted when the user successfully connected to the server
         * with the oauth method.
         *
         * @property method The method that was used for authentication.
         */
        data class Authenticated(
            val accessToken: String,
            val method: AuthMethod.OAuthMethod,
            val refreshToken: String? = null,
        ) : Output

        /**
         * Output that is emitted when an authentication error occurs, for example, in case
         * of timeout or invalid configuration.
         *
         * @property error The error that was thrown during a connection establishment.
         */
        data class AuthenticationFailed(val error: Throwable) : Output

        /**
         * Output that allows resetting any error reported via [AuthenticationFailed].
         */
        data object ErrorReset : Output
    }
}
