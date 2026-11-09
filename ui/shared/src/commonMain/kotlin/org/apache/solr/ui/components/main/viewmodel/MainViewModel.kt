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

package org.apache.solr.ui.components.main.viewmodel

import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.snapshots.Snapshot
import androidx.compose.runtime.snapshots.SnapshotStateList
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.navigation3.runtime.NavKey
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.main.domain.MainEvent
import org.apache.solr.ui.views.navigation.MainMenu

/**
 * View model of the main section that is used as base for users with access.
 *
 * Note that this section can be accessed if the user is either authenticated or if the Solr
 * instance accessed does not have any authentication enabled.
 *
 * @param destination The section to show initially. Falls back to the configsets section if the
 * destination is missing or not supported.
 */
class MainViewModel(destination: String? = null) : ViewModel() {

    /**
     * The back stack that holds the navigation state.
     */
    val backStack: SnapshotStateList<MainScene> = mutableStateListOf(initialScene(destination))

    /**
     * Events emitted by the view model.
     */
    val events: SharedFlow<MainEvent>
        field = MutableSharedFlow<MainEvent>(extraBufferCapacity = 1)

    /**
     * Handles navigation requests from a navigation menu. The destination is brought to the front
     * of the back stack, so that returning to a section keeps its state.
     *
     * @param menuItem The destination to navigate to.
     */
    fun navigate(menuItem: MainMenu) {
        val scene = menuItem.toScene()
        if (backStack.lastOrNull() == scene) return

        Snapshot.withMutableSnapshot {
            backStack.remove(scene)
            backStack.add(scene)
        }
    }

    /**
     * Navigates back to the previous section, if there is any.
     */
    fun navigateBack() {
        if (backStack.size > 1) backStack.removeLastOrNull()
    }

    /**
     * Handles logout requests.
     */
    fun logout() {
        viewModelScope.launch { events.emit(MainEvent.UserLoggedOut) }
    }

    /**
     * Calculates the initial scene based on the destination provided.
     */
    private fun initialScene(destination: String?): MainScene = when (destination) {
        "cluster" -> MainScene.Cluster
        "configsets" -> MainScene.Configsets
        "environment" -> MainScene.Environment
        "logging" -> MainScene.Logging
        else -> MainScene.Configsets
    }
}

/**
 * The sections of the main screen that can be navigated to.
 */
@Serializable
sealed interface MainScene : NavKey {

    @Serializable
    data object Cluster : MainScene

    @Serializable
    data object Configsets : MainScene

    @Serializable
    data object Environment : MainScene

    @Serializable
    data object Logging : MainScene
}

/**
 * Maps a menu item to the scene it leads to.
 */
private fun MainMenu.toScene(): MainScene = when (this) {
    // TODO Add additional mappings once more sections are supported
    MainMenu.Cluster -> MainScene.Cluster
    MainMenu.Configsets -> MainScene.Configsets
    MainMenu.Environment -> MainScene.Environment
    MainMenu.Logging -> MainScene.Logging

    // TODO Remove else case once all destinations are available
    else -> throw NotImplementedError("Navigation to $this not implemented yet.")
}

/**
 * The menu item that leads to this scene.
 */
val MainScene.menuItem: MainMenu
    get() = when (this) {
        MainScene.Cluster -> MainMenu.Cluster
        MainScene.Configsets -> MainMenu.Configsets
        MainScene.Environment -> MainMenu.Environment
        MainScene.Logging -> MainMenu.Logging
    }
