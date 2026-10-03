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

package org.apache.solr.ui.components.auth.domain

import io.ktor.http.Url
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption

/**
 * Events emitted by the authentication screen that are meant to be handled by the parent.
 */
sealed interface AuthenticationEvent {

    /**
     * Emitted when the user successfully authenticated against the Solr instance.
     *
     * @property option The final authentication option that succeeded.
     */
    data class Authenticated(val option: AuthOption) : AuthenticationEvent

    /**
     * Emitted when the user aborts the authentication flow.
     */
    data object Aborted : AuthenticationEvent
}

/**
 * Events emitted by the basic authentication state holder.
 */
sealed interface BasicAuthEvent {

    /**
     * Emitted when a connection process is started.
     */
    data object AuthenticationStarted : BasicAuthEvent

    /**
     * Emitted when the user successfully connected to the server with the credentials.
     *
     * @property method The method that was used for authentication.
     * @property username The username that was used for authentication.
     * @property password The password that was used for authentication.
     */
    data class Authenticated(
        val method: AuthMethod.BasicAuthMethod,
        val username: String,
        val password: String,
    ) : BasicAuthEvent

    /**
     * Emitted when an authentication error occurs, for example, in case of invalid credentials.
     *
     * @property error The error that occurred during the connection establishment.
     */
    data class AuthenticationFailed(val error: Throwable) : BasicAuthEvent

    /**
     * Emitted when an error reported via [AuthenticationFailed] is reset.
     */
    data object ErrorReset : BasicAuthEvent
}

/**
 * Events emitted by the OAuth state holder.
 */
sealed interface OAuthEvent {

    /**
     * Emitted when a connection process is started.
     *
     * @property url The URL of the identity provider to open for authentication.
     */
    data class AuthenticationStarted(val url: Url) : OAuthEvent

    /**
     * Emitted when the user successfully connected to the server with the OAuth method.
     *
     * @property method The method that was used for authentication.
     * @property accessToken The access token that was issued.
     * @property refreshToken The refresh token that was issued, if any.
     */
    data class Authenticated(
        val method: AuthMethod.OAuthMethod,
        val accessToken: String,
        val refreshToken: String? = null,
    ) : OAuthEvent

    /**
     * Emitted when an authentication error occurs, for example, in case of timeout or invalid
     * configuration.
     *
     * @property error The error that occurred during the connection establishment.
     */
    data class AuthenticationFailed(val error: Throwable) : OAuthEvent
}
