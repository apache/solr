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

import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.domain.AuthMethod

/**
 * Component interface that is handling basic authentication with username and password.
 */
interface BasicAuthComponent {

    val model: StateFlow<Model>

    /**
     * Function that handles username changes.
     */
    fun onChangeUsername(username: String)

    /**
     * Function that handles password changes.
     */
    fun onChangePassword(password: String)

    /**
     * Tries to authenticate the user with the given username and password.
     */
    fun onAuthenticate()

    /**
     * Data class model that represents the [BasicAuthComponent] state.
     *
     * @property realm The basic auth realm the user is supposed to provide the credentials for.
     * @property username The username that is currently typed in.
     * @property password The password that is currently typed in.
     * @property hasError Whether the current input has an error. This is a simplified variant that
     * does not distinguish input fields.
     */
    data class Model(
        val realm: String = "",
        val username: String = "",
        val password: String = "",
        val hasError: Boolean = false,
    )

    sealed interface Output {

        /**
         * Output that is emitted when a connection process is started.
         */
        data object Authenticating : Output

        /**
         * Output that is emitted when the user successfully connected to the server
         * with the credentials.
         *
         * The username and password are passed to the parent to configure a client
         * with basic auth and pass it to other instances for further use.
         *
         * @property method The method that was used for authentication.
         * @property username The username that was used for authentication.
         * @property password The password that was used for authentication.
         */
        data class Authenticated(
            val method: AuthMethod.BasicAuthMethod,
            val username: String,
            val password: String,
        ) : Output

        /**
         * Output that is emitted when an authentication error occurs, for example, in case
         * of invalid credentials.
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
