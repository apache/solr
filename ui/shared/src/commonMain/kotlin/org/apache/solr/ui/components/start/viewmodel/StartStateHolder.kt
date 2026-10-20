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

package org.apache.solr.ui.components.start.viewmodel

import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.http.URLParserException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.start.domain.ConnectResult
import org.apache.solr.ui.components.start.domain.ConnectUseCase
import org.apache.solr.ui.components.start.domain.StartEvent
import org.apache.solr.ui.errors.HostNotFoundException
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.error_invalid_url
import org.apache.solr.ui.shared.generated.resources.error_solr_host_not_found
import org.apache.solr.ui.shared.generated.resources.error_unknown
import org.apache.solr.ui.utils.AppDispatchers
import org.jetbrains.compose.resources.StringResource

/**
 * State holder of the start screen that manages the Solr URL and connection attempts.
 */
class StartStateHolder(
    private val scope: CoroutineScope,
    private val connectUseCase: ConnectUseCase,
    private val dispatchers: AppDispatchers,
) {

    /**
     * UI state of the start screen.
     */
    val uiState: StateFlow<StartUiState>
        field = MutableStateFlow(StartUiState())

    /**
     * Events emitted by the state holder.
     */
    val events: SharedFlow<StartEvent>
        field = MutableSharedFlow<StartEvent>(extraBufferCapacity = 1)

    /**
     * Updates the Solr URL and resets any error.
     *
     * @param url The new Solr URL value.
     */
    fun changeSolrUrl(url: String) = uiState.update { it.copy(url = url, error = null) }

    /**
     * Connects to the Solr instance with the current [StartUiState.url].
     */
    fun connect() {
        scope.launch {
            val result = withContext(dispatchers.io) {
                connectUseCase(uiState.value.url)
            }

            when (result) {
                is ConnectResult.Connected -> events.emit(StartEvent.Connected(result.url))

                is ConnectResult.AuthRequired -> events.emit(
                    StartEvent.AuthRequired(url = result.url, methods = result.methods),
                )

                is ConnectResult.Failure -> uiState.update {
                    it.copy(error = result.error.toErrorResource())
                }
            }
        }
    }
}

/**
 * UI state of the start screen.
 *
 * @property url The Solr URL entered by the user.
 * @property isConnecting Whether a connection is currently being established.
 * @property error The error that occurred during the last connection attempt, if any.
 */
data class StartUiState(
    val url: String = "",
    val isConnecting: Boolean = false,
    val error: StringResource? = null,
)

private fun Throwable.toErrorResource(): StringResource = when (this) {
    is URLParserException -> Res.string.error_invalid_url
    is HostNotFoundException -> Res.string.error_solr_host_not_found
    is ConnectTimeoutException -> Res.string.error_solr_host_not_found
    else -> Res.string.error_unknown
}
