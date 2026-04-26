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
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update

class ConfigsetsOverviewViewModel : ViewModel() {

    /**
     * The dialog that is currently open, if any.
     */
    val uiState: StateFlow<ConfigsetsOverviewUiState>
        field = MutableStateFlow(ConfigsetsOverviewUiState())

    /**
     * Initiates the creation of a new configset.
     */
    fun createConfigset() = uiState.update {
        it.copy(dialogState = DialogState.CreateConfigsetDialog)
    }

    /**
     * Initiates the import of a configset.
     */
    fun importConfigset() = uiState.update {
        it.copy(dialogState = DialogState.ImportConfigsetDialog)
    }

    /**
     * Closes any opened dialog.
     */
    fun closeDialog() = uiState.update { it.copy(dialogState = null) }

    /**
     * Edit solrconfig.xml for the configset with the given [name].
     *
     * @param name the name of the configset to edit.
     */
    fun editSolrConfig(name: String) {
        TODO()
    }
}

data class ConfigsetsOverviewUiState(
    val dialogState: DialogState? = null,
)

sealed interface DialogState {

    // TODO Consider adding configsets and current configset selection as values to this class
    data object CreateConfigsetDialog : DialogState

    data object ImportConfigsetDialog : DialogState
}
