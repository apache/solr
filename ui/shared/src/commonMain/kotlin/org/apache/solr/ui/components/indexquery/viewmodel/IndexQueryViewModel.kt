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

package org.apache.solr.ui.components.indexquery.viewmodel

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.apache.solr.ui.components.indexquery.data.IndexQueryRepository
import org.apache.solr.ui.domain.RequestHandlerOption

data class IndexQueryUiState(
    val collections: List<String> = emptyList(),
    val collectionsLoading: Boolean = false,
    val selectedCollection: String? = null,
    val handlers: List<RequestHandlerOption> = emptyList(),
    val handlersLoading: Boolean = false,
    val selectedHandler: String? = null,
    val error: Boolean = false,
)

/** Loads and manages the Index & Query header state. */
class IndexQueryViewModel internal constructor(
    private val repository: IndexQueryRepository,
    private val testScope: CoroutineScope?,
) : ViewModel() {
    constructor(repository: IndexQueryRepository) : this(repository, null)

    val uiState: StateFlow<IndexQueryUiState>
        field = MutableStateFlow(IndexQueryUiState())

    private val scope get() = testScope ?: viewModelScope
    private var handlersJob: Job? = null

    init {
        loadCollections()
    }

    private fun loadCollections() {
        uiState.update { it.copy(collectionsLoading = true, error = false) }
        scope.launch {
            repository.listCollections()
                .onSuccess { names ->
                    uiState.update { it.copy(collections = names, collectionsLoading = false) }
                }
                .onFailure {
                    uiState.update { it.copy(collectionsLoading = false, error = true) }
                }
        }
    }

    fun selectCollection(collection: String) {
        handlersJob?.cancel()
        uiState.update {
            it.copy(
                selectedCollection = collection,
                handlers = emptyList(),
                handlersLoading = true,
                error = false,
            )
        }
        handlersJob = scope.launch {
            repository.listRequestHandlers(collection)
                .onSuccess { handlers ->
                    uiState.update { state ->
                        state.copy(
                            handlers = handlers,
                            handlersLoading = false,
                            selectedHandler = state.selectedHandler?.takeIf { selected ->
                                handlers.any { it.name == selected }
                            },
                        )
                    }
                }
                .onFailure {
                    uiState.update {
                        it.copy(
                            handlersLoading = false,
                            selectedHandler = null,
                            error = true,
                        )
                    }
                }
        }
    }

    fun selectHandler(handler: String) {
        uiState.update { it.copy(selectedHandler = handler) }
    }
}
