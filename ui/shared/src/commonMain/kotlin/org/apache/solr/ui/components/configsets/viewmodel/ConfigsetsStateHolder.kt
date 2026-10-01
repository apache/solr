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

package org.apache.solr.ui.components.configsets.viewmodel

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.utils.AppDispatchers

class ConfigsetsStateHolder(
    private val scope: CoroutineScope,
    private val loadConfigsetsUseCase: LoadConfigsetsUseCase,
    private val dispatchers: AppDispatchers,
) {

    /**
     * State of the configset create form.
     */
    val uiState: StateFlow<ConfigsetsUiState>
        field = MutableStateFlow(ConfigsetsUiState())

    init {
        loadConfigsets()
    }

    /**
     * Selects the given [configset].
     *
     * @param configset The configset to select.
     */
    fun selectConfigset(configset: String) = uiState.update {
        it.copy(selectedConfigset = configset)
    }

    /**
     * Clears the currently selected configset, if any.
     */
    fun clearSelectedConfigset() = uiState.update { it.copy(selectedConfigset = null) }

    /**
     * Reloads the configsets.
     */
    fun reloadConfigsets() {
        loadConfigsets(clearOnFailure = true)
    }

    private fun loadConfigsets(clearOnFailure: Boolean = false) = scope.launch {
        withContext(dispatchers.io) {
            loadConfigsetsUseCase()
        }.onSuccess { configsets ->
            uiState.update { it.copy(configsets = configsets.sortedBy(Configset::name)) }
            if (configsets.none { it.name == uiState.value.selectedConfigset }) {
                // Unselect current configset
                clearSelectedConfigset()
            }
        }.onFailure {
            // TODO Notify user about loading issue
            if (clearOnFailure) {
                uiState.update {
                    it.copy(
                        configsets = emptyList(),
                        selectedConfigset = null,
                    )
                }
            }
        }
    }
}

data class ConfigsetsUiState(
    val configsets: List<Configset> = emptyList(),
    val selectedConfigset: String? = null,
)
