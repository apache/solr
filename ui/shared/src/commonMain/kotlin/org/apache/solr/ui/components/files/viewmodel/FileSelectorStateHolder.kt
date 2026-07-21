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

package org.apache.solr.ui.components.files.viewmodel

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.apache.solr.ui.components.files.domain.FileSelectorEvent
import org.apache.solr.ui.components.files.domain.SelectFileResult
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.domain.PickedFile

class FileSelectorStateHolder(
    private val scope: CoroutineScope,
    private val selectFileUseCase: SelectFileUseCase,
) {

    /**
     * State of the configset create form.
     */
    val uiState: StateFlow<FileSelectorUiState>
        field = MutableStateFlow(FileSelectorUiState())

    /**
     * Events emitted by the state holer.
     */
    val events: SharedFlow<FileSelectorEvent>
        field = MutableSharedFlow<FileSelectorEvent>(extraBufferCapacity = 1)

    fun selectFile(extensions: List<String>) {
        uiState.update { it.copy(fileError = null) }
        scope.launch {
            when (val result = selectFileUseCase(extensions)) {
                is SelectFileResult.Aborted -> Unit

                // Ignore
                is SelectFileResult.Success -> {
                    uiState.update {
                        it.copy(file = result.file, fileError = null)
                    }
                    events.emit(FileSelectorEvent.FileSelected(result.file))
                }

                is SelectFileResult.ValidationFailure ->
                    uiState.update { it.copy(fileError = result.error) }

                is SelectFileResult.UnexpectedFailure -> {
                    // TODO Handle general error
                }
            }
        }
    }

    fun clearFile() {
        uiState.update { it.copy(file = null) }
    }

    fun validateAndGetFile(): PickedFile? {
        var isValid = true
        if (uiState.value.file == null) {
            uiState.update { it.copy(fileError = SelectFileResult.Error.FileNotSelected) }
            isValid = false
        }
        if (isValid) return uiState.value.file
        return null
    }
}

data class FileSelectorUiState(
    val file: PickedFile? = null,
    val fileError: SelectFileResult.Error? = null,
)
