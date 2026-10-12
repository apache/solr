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
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Intent
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Label
import org.apache.solr.ui.components.auth.store.BasicAuthStore.State
import org.apache.solr.ui.domain.AuthMethod.BasicAuthMethod

interface BasicAuthStore : Store<Intent, State, Label> {

    /**
     * Intent for interacting with the basic auth store.
     */
    sealed interface Intent {

        /**
         * Intent for updating the username value.
         */
        data class UpdateUsername(val username: String) : Intent

        /**
         * Intent for updating the password value.
         */
        data class UpdatePassword(val password: String) : Intent

        /**
         * Intent for initiating an authentication attempt with the current credentials.
         */
        data object Authenticate : Intent
    }

    sealed interface Label {

        /**
         * Label that is published when a new authentication process starts.
         */
        data object AuthenticationStarted : Label

        /**
         * Label that is published when the user successfully authenticated to the Solr instance.
         *
         * @property method The method that was used for authentication.
         * @property username The username that was used for authentication.
         * @property password The password that was used for authentication.
         */
        data class Authenticated(
            val method: BasicAuthMethod,
            val username: String,
            val password: String,
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
     * State class that holds the data of the [BasicAuthStore].
     */
    data class State(
        val method: BasicAuthMethod = BasicAuthMethod(),
        val username: String = "",
        val password: String = "",
        val error: Throwable? = null,
    )
}
