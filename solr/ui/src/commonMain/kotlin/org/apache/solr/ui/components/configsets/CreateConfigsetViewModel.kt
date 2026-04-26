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

package org.apache.solr.ui.components.configsets

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetResult
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.domain.Configset

class CreateConfigsetViewModel(
    private val createConfigsetUseCase: CreateConfigsetUseCase,
    private val loadConfigsetsUseCase: LoadConfigsetsUseCase,
    private val ioDispatcher: CoroutineDispatcher, // TODO Change to AppDispatchers instead
    configsets: List<Configset> = emptyList(),
    selectedBaseConfigSet: String? = null,
) : ViewModel() {

    /**
     * State of the configset create form.
     */
    val uiState: StateFlow<CreateConfigsetFormUiState>
        field = MutableStateFlow(
            value = CreateConfigsetFormUiState(
                configsets = configsets,
                selectedBaseConfigset = selectedBaseConfigSet,
            ),
        )

    /**
     * Events emitted by the viewmodel.
     */
    val events: SharedFlow<CreateConfigsetEvent>
        field = MutableSharedFlow<CreateConfigsetEvent>(extraBufferCapacity = 1)

    init {
        loadConfigsets()
    }

    fun changeConfigsetName(configsetName: String) = uiState.update {
        it.copy(configsetName = configsetName)
    }

    fun changeBaseConfigset(baseConfigset: String) = uiState.update {
        it.copy(selectedBaseConfigset = baseConfigset)
    }

    fun createConfigset() {
        uiState.update { it.copy(isLoading = true) }
        // TODO Validate input data or let use case validate data

        viewModelScope.launch(context = ioDispatcher) {
            when (val result = createConfigsetUseCase(
                configsetName = uiState.value.configsetName,
                baseConfigset = uiState.value.selectedBaseConfigset,
            )) {
                is CreateConfigsetResult.Success ->
                    events.emit(CreateConfigsetEvent.ConfigsetCreated(result.configset))
                is CreateConfigsetResult.ValidationFailure ->
                    uiState.update { it.copy(configsetNameError = result.error) }
                is CreateConfigsetResult.UnexpectedFailure -> TODO()
            }
        }
    }

    fun clearBaseConfigset() = uiState.update { it.copy(selectedBaseConfigset = null) }

    private fun loadConfigsets() {
        viewModelScope.launch {
            withContext(ioDispatcher) {
                loadConfigsetsUseCase()
            }.onSuccess { configsets ->
                uiState.update { it.copy(configsets = configsets) }
            }.onFailure {
                // TODO Add proper error handling
            }
        }
    }
}

data class CreateConfigsetFormUiState(
    val configsetName: String = "",
    val configsetNameError: CreateConfigsetResult.Error? = null,
    val configsets: List<Configset> = emptyList(),
    val selectedBaseConfigset: String? = null,
    val isLoading: Boolean = false,
)
