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

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.Value
import io.ktor.http.Url
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.Serializable
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption
import org.jetbrains.compose.resources.StringResource

/**
 * The authentication component takes care of the authentication processes. This typically includes
 * user authentication with credentials, tokens or certificates.
 */
interface AuthenticationComponent {

    /**
     * Component state flow.
     */
    val model: StateFlow<Model>

    /**
     * A child slot that holds the [BasicAuthComponent] for user authentication with credentials
     * (basic auth) if it is supported.
     */
    val basicAuthSlot: Value<ChildSlot<BasicAuthConfiguration, BasicAuthComponent>>

    @Serializable
    data class BasicAuthConfiguration(val method: AuthMethod.BasicAuthMethod = AuthMethod.BasicAuthMethod())

    /**
     * Aborts the authentication attempt.
     */
    fun onAbort()

    /**
     * Model data class that represents the component state.
     *
     * @property url URL of the Solr instance the user is trying to connect.
     * @property methods List of authentication methods to render.
     * @property isAuthenticating Whether a connection is currently established.
     * @property error The error that may have occurred.
     */
    data class Model(
        val url: String = "",
        val methods: List<AuthMethod> = emptyList(),
        val isAuthenticating: Boolean = false,
        val error: StringResource? = null,
    )

    sealed interface Output {

        /**
         * Emitted when the user successfully authenticated against the Solr instance.
         *
         * @property option The final authentication option that succeeded.
         */
        data class OnAuthenticated(val option: AuthOption) : Output

        /**
         * Emitted when the user aborts the authentication flow.
         */
        data object OnAbort : Output
    }
}
