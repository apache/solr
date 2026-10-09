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

import com.arkivanov.mvikotlin.core.store.Store
import io.ktor.http.Url
import org.apache.solr.ui.components.auth.store.OAuthStore.Intent
import org.apache.solr.ui.components.auth.store.OAuthStore.Label
import org.apache.solr.ui.components.auth.store.OAuthStore.State
import org.apache.solr.ui.domain.AuthMethod.OAuthMethod

interface OAuthStore : Store<Intent, State, Label> {

    /**
     * Intent for interacting with the OAuth store.
     */
    sealed interface Intent {

        /**
         * Intent for initiating an authentication attempt with the current OAuth configuration.
         */
        data object Authenticate : Intent
    }

    sealed interface Label {

        /**
         * Label that is published when a new authentication process starts.
         *
         * @property url The URL of the identity provider to open for authentication.
         */
        data class AuthenticationStarted(val url: Url) : Label

        /**
         * Label that is published when the user successfully authenticated to the Solr instance.
         *
         * @property method The method that was used for authentication.
         */
        data class Authenticated(
            val method: OAuthMethod,
            val accessToken: String,
            val refreshToken: String? = null,
        ) : Label

        /**
         * Label that is published when the authentication process failed with an error.
         *
         * @property error The error that occurred during the authentication process.
         */
        data class AuthenticationFailed(val error: Throwable) : Label

        /**
         * Label that is published whenever the error state resets.
         */
        data object ErrorReset : Label
    }

    /**
     * State class that holds the data of the [OAuthStore].
     */
    data class State(
        val method: OAuthMethod,
        val error: Throwable? = null,
    )
}
