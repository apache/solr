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

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetEvent
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetResult
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.files.domain.FileSelectorEvent
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.components.files.viewmodel.FileSelectorStateHolder
import org.apache.solr.ui.domain.PickedFile
import org.apache.solr.ui.utils.AppDispatchers

class ImportConfigsetViewModel(
    private val importConfigsetUseCase: ImportConfigsetUseCase,
    selectFileUseCase: SelectFileUseCase,
    private val dispatchers: AppDispatchers,
) : ViewModel() {

    private val fileSelectorState = FileSelectorStateHolder(
        scope = viewModelScope,
        selectFileUseCase = selectFileUseCase,
    )

    /**
     * State of the configset import form.
     */
    val uiState: StateFlow<ImportConfigsetUiState>
        field = MutableStateFlow(ImportConfigsetUiState())

    /**
     * State of the file selector.
     */
    val fileSelectorUiState = fileSelectorState.uiState

    /**
     * Events emitted by the viewmodel. This events flow uses the same events as
     * [CreateConfigsetViewModel] for simplicity.
     */
    val events: SharedFlow<CreateConfigsetEvent>
        field = MutableSharedFlow<CreateConfigsetEvent>(extraBufferCapacity = 1)

    init {
        viewModelScope.launch {
            fileSelectorState.events.collect { event ->
                when (event) {
                    is FileSelectorEvent.FileSelected -> if(!uiState.value.configsetNameChanged) {
                        setFileNameAsConfigset(event.file)
                    }
                }
            }
        }
    }

    fun changeConfigsetName(name: String) = uiState.update {
        it.copy(configsetName = name, configsetNameChanged = true)
    }

    fun selectFile() = fileSelectorState.selectFile(SUPPORTED_CONFIGSET_IMPORT_FILE_EXTENSIONS)

    fun clearFile() = fileSelectorState.clearFile()

    fun importConfigset() {
        val file = fileSelectorState.validateAndGetFile() ?: return

        uiState.update { it.copy(isLoading = true) }
        // TODO Validate input data or let use case validate data

        viewModelScope.launch {
            val result = withContext(context = dispatchers.io) {
                importConfigsetUseCase(uiState.value.configsetName, file)
            }

            when (result) {
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

    fun toggleInput() = viewModelScope.launch {
        events.emit(CreateConfigsetEvent.ConfigsetCreateToggleInputForm(useFileInput = false))
    }

    fun abortImport() = viewModelScope.launch {
        events.emit(CreateConfigsetEvent.ConfigsetCreationAborted)
    }

    /**
     * Update the configset name to use the file name as configset name, removing the
     */
    private fun setFileNameAsConfigset(file: PickedFile) {
        uiState.update {
            it.copy(
                configsetName = file.name
                    .removeSuffix(".${file.extension ?: "zip"}")
                    .removeSuffix("_configset"),
            )
        }
    }
}

/**
 * A list of the supported file extensions for configsets that can be imported.
 *
 * Currently only zip files are supported.
 */
private val SUPPORTED_CONFIGSET_IMPORT_FILE_EXTENSIONS = listOf("zip")

data class ImportConfigsetUiState(
    val configsetName: String = "",
    val configsetNameError: ImportConfigsetResult.Error? = null,
    val configsetNameChanged: Boolean = false,
    val isLoading: Boolean = false,
)
