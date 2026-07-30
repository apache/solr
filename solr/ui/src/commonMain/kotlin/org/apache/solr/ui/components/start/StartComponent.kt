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

package org.apache.solr.ui.components.start

import io.ktor.http.Url
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.domain.AuthMethod
import org.jetbrains.compose.resources.StringResource

/**
 * Component interface that represents the start screen.
 */
interface StartComponent {

    val model: StateFlow<Model>

    /**
     * Function for when the input value for the Solr URL changes.
     *
     * @param url The new URL value
     */
    fun onSolrUrlChange(url: String)

    /**
     * Called when the user wants to connect with the current [Model.url].
     */
    fun onConnect()

    /**
     * State class that holds values of the [StartComponent]'s sate.
     */
    data class Model(
        val url: String = "",
        val isConnecting: Boolean = false,
        val error: StringResource? = null,
    )

    /**
     * Possible component outputs that may be emitted to the parent component.
     */
    sealed interface Output {

        /**
         * Emitted when a connection to a Solr instance has been
         * established and no authentication is required.
         *
         * @property url The URL the connection was established.
         */
        data class OnConnected(val url: Url) : Output

        /**
         * Emitted when a connection to a Solr instance has been established and authentication
         * is needed.
         *
         * @property url The URL the connection was established but requires authentication.
         * @property methods List of authentication methods that can be used for authenticating.
         */
        data class OnAuthRequired(val url: Url, val methods: List<AuthMethod>) : Output
    }
}
