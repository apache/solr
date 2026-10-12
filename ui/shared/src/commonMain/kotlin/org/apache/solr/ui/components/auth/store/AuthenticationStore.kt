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
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Label
import org.apache.solr.ui.components.auth.store.AuthenticationStore.State
import org.apache.solr.ui.domain.AuthMethod

interface AuthenticationStore : Store<Intent, State, Label> {

    sealed interface Intent {

        /**
         * Intent for indicating that an authentication flow is in process.
         */
        data object StartAuthenticating : Intent

        /**
         * Intent for when an authentication error occurs.
         */
        data class FailAuthentication(val error: Throwable) : Intent

        /**
         * Intent for resetting any errors set via [FailAuthentication].
         */
        data object ResetError : Intent
    }

    sealed interface Label {
        // TODO Add labels for the authentication store
    }

    /**
     * State class that holds the data of the [AuthenticationStore].
     *
     * @property methods Authentication methods that are configured and can be used
     * for authentication.
     * @property isAuthenticating Whether an authentication process is currently running.
     * @property error The authentication error in case something went wrong.
     */
    data class State(
        val url: Url = Url(""),
        val methods: List<AuthMethod> = emptyList(),
        val isAuthenticating: Boolean = false,
        val error: Throwable? = null,
    )
}
