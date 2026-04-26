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

import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update

class ConfigsetsSceneViewModel(savedStateHandle: SavedStateHandle) : ViewModel() {

    private val initialTab = savedStateHandle
        .get<String>("tab")
        ?.let { ConfigsetTab.valueOf(it) }
        ?: ConfigsetTab.ConfigsetOverview

    /**
     * The dialog that is currently open, if any.
     */
    val uiState: StateFlow<ConfigsetsSceneUiState>
        field = MutableStateFlow(ConfigsetsSceneUiState(selectedTab = initialTab))

    /**
     * Switches to the configset [tab] that was provided.
     *
     * @param tab The tab to select.
     */
    fun selectTab(tab: ConfigsetTab) = uiState.update { it.copy(selectedTab = tab) }
}

data class ConfigsetsSceneUiState(
    val selectedTab: ConfigsetTab = ConfigsetTab.ConfigsetOverview,
)

enum class ConfigsetTab {
    ConfigsetOverview,
    Placeholder,

    // TODO Add additional Configset tabs
}
