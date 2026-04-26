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
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetResult
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.files.domain.SelectFileResult
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.domain.PickedFile

class ImportConfigsetViewModel(
    private val importConfigsetUseCase: ImportConfigsetUseCase,
    private val selectFileUseCase: SelectFileUseCase,
    private val ioDispatcher: CoroutineDispatcher, // TODO Change to AppDispatchers instead
) : ViewModel() {
    /**
     * State of the configset create form.
     */
    val uiState: StateFlow<ImportConfigsetUiState>
        field = MutableStateFlow(ImportConfigsetUiState())

    /**
     * Events emitted by the viewmodel.
     */
    val events: SharedFlow<CreateConfigsetEvent>
        field = MutableSharedFlow<CreateConfigsetEvent>(extraBufferCapacity = 1)

    fun changeConfigsetName(name: String) = uiState.update {
        it.copy(configsetName = name, configsetNameChanged = true)
    }

    fun selectFile(extensions: List<String>) {
        uiState.update { it.copy(fileError = null) }
        viewModelScope.launch {
            when (val result = selectFileUseCase(extensions)) {
                is SelectFileResult.Aborted -> Unit // Ignore
                is SelectFileResult.Success -> uiState.update {
                    it.copy(
                        configsetName = if (it.configsetNameChanged) it.configsetName
                        else result.file.name
                            .removeSuffix(".${result.file.extension}")
                            .removeSuffix("_configset"),
                        file = result.file,
                    )
                }

                is SelectFileResult.ValidationFailure -> uiState.update { it.copy(fileError = result.error) }
                is SelectFileResult.UnexpectedFailure -> {
                    // TODO Handle general error
                }
            }
        }
    }

    fun clearFile() {
        uiState.update { it.copy(file = null) }
    }

    fun importConfigset() {
        val file = uiState.value.file ?: run {
            uiState.update { it.copy(fileError = SelectFileResult.Error.FileNotSelected) }
            return
        }
        uiState.update { it.copy(isLoading = true) }
        // TODO Validate input data or let use case validate data

        viewModelScope.launch(context = ioDispatcher) {
            when (val result = importConfigsetUseCase(uiState.value.configsetName, file)) {
                is ImportConfigsetResult.Success ->
                    events.emit(CreateConfigsetEvent.ConfigsetCreated(result.configset))

                is ImportConfigsetResult.ValidationFailure -> uiState.update {
                    it.copy(
                        configsetNameError = when (val error = result.error) {
                            ImportConfigsetResult.Error.InvalidConfigsetName,
                            ImportConfigsetResult.Error.DuplicateConfigset -> error
                        },
                    )
                }

                is ImportConfigsetResult.UnexpectedFailure -> TODO()
            }
        }
    }
}

data class ImportConfigsetUiState(
    val configsetName: String = "",
    val configsetNameError: ImportConfigsetResult.Error? = null,
    val configsetNameChanged: Boolean = false,
    val file: PickedFile? = null,
    val fileError: SelectFileResult.Error? = null,
    val isLoading: Boolean = false,
)
