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

import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.snapshots.SnapshotStateList
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.navigation3.runtime.NavKey
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.serialization.Serializable

// TODO Make savedStateHandle mandatory
class ConfigsetsRouteViewModel(
    savedStateHandle: SavedStateHandle? = null,
) : ViewModel() {

    private val initialTab = savedStateHandle
        ?.get<String>("tab")
        ?.let { ConfigsetsTab.valueOf(it) }
        ?: ConfigsetsTab.Overview

    val backStack: SnapshotStateList<ConfigsetsScene> = mutableStateListOf(
        when(initialTab) {
            ConfigsetsTab.Overview -> ConfigsetsScene.Overview
            ConfigsetsTab.Files -> ConfigsetsScene.Files
            ConfigsetsTab.Schema -> ConfigsetsScene.Schema
            ConfigsetsTab.UpdateConfig -> ConfigsetsScene.UpdateConfig
            ConfigsetsTab.IndexQuery -> ConfigsetsScene.IndexQuery
            ConfigsetsTab.Handlers -> ConfigsetsScene.Handlers
            ConfigsetsTab.SearchComponents -> ConfigsetsScene.SearchComponents
        },
    )

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
    fun selectTab(tab: ConfigsetsTab) = uiState.update { it.copy(selectedTab = tab) }
}

data class ConfigsetsSceneUiState(
    val selectedTab: ConfigsetsTab = ConfigsetsTab.Overview,
)

@Serializable
sealed interface ConfigsetsScene : NavKey {

    @Serializable
    data object Overview : ConfigsetsScene

    @Serializable
    data object Files : ConfigsetsScene

    @Serializable
    data object Schema : ConfigsetsScene

    @Serializable
    data object UpdateConfig : ConfigsetsScene

    @Serializable
    data object IndexQuery : ConfigsetsScene

    @Serializable
    data object Handlers : ConfigsetsScene

    @Serializable
    data object SearchComponents : ConfigsetsScene
}

@Serializable
enum class ConfigsetsTab {
    Overview,
    Files,
    Schema,
    UpdateConfig,
    IndexQuery,
    Handlers,
    SearchComponents,
}
