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

import kotlinx.coroutines.CoroutineScope
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

class IndexQueryStateHolder(
    private val repository: IndexQueryRepository,
    private val scope: CoroutineScope,
) {
    val uiState: StateFlow<IndexQueryUiState>
        field = MutableStateFlow(IndexQueryUiState())

    fun loadCollections() {
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
        uiState.update {
            it.copy(
                selectedCollection = collection,
                handlers = emptyList(),
                handlersLoading = true,
                selectedHandler = null,
                error = false,
            )
        }
        scope.launch {
            repository.listRequestHandlers(collection)
                .onSuccess { handlers ->
                    uiState.update { state ->
                        if (state.selectedCollection == collection) {
                            state.copy(handlers = handlers, handlersLoading = false)
                        } else {
                            state
                        }
                    }
                }
                .onFailure {
                    uiState.update { state ->
                        if (state.selectedCollection == collection) {
                            state.copy(handlersLoading = false, error = true)
                        } else {
                            state
                        }
                    }
                }
        }
    }

    fun selectHandler(handler: String) {
        uiState.update { it.copy(selectedHandler = handler) }
    }
}
