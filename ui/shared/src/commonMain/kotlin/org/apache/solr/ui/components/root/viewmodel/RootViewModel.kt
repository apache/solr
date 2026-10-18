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

package org.apache.solr.ui.components.root.viewmodel

import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.snapshots.Snapshot
import androidx.compose.runtime.snapshots.SnapshotStateList
import androidx.lifecycle.ViewModel
import androidx.navigation3.runtime.NavKey
import io.ktor.http.Url
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.auth.domain.AuthenticationEvent
import org.apache.solr.ui.components.main.domain.MainEvent
import org.apache.solr.ui.components.start.domain.StartEvent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption

/**
 * View model that manages the navigation between the top-level screens of the application.
 *
 * The view model checks the events emitted by the screens and redirects the user accordingly.
 * This implementation does not check the user's access level and redirects to the requested
 * destination. It is used only temporary and will be replaced in the future with an implementation
 * that checks the access level of the user before redirecting.
 */
class RootViewModel : ViewModel() {

    /**
     * The back stack that holds the navigation state.
     */
    val backStack: SnapshotStateList<RootScene> = mutableStateListOf(RootScene.Start)

    /**
     * Event handler for any event emitted by the start screen.
     *
     * @param event The event emitted by the start screen.
     */
    fun onStartEvent(event: StartEvent) = when (event) {
        is StartEvent.AuthRequired -> backStack.add(
            RootScene.Authentication(
                url = event.url,
                methods = event.methods,
            ),
        )

        is StartEvent.Connected ->
            replaceAll(RootScene.Main(authOption = AuthOption.None(url = event.url)))
    }

    /**
     * Event handler for any event emitted by the authentication screen.
     *
     * @param event The event emitted by the authentication screen.
     */
    fun onAuthenticationEvent(event: AuthenticationEvent) = when (event) {
        is AuthenticationEvent.Authenticated ->
            replaceAll(RootScene.Main(authOption = event.option))

        is AuthenticationEvent.Aborted -> navigateBack()
    }

    /**
     * Event handler for any event emitted by the main screen.
     *
     * @param event The event emitted by the main screen.
     */
    fun onMainEvent(event: MainEvent) = when (event) {
        is MainEvent.UserLoggedOut -> replaceAll(RootScene.Start)
    }

    /**
     * Navigates back to the previous screen, if there is any.
     */
    fun navigateBack() {
        if (backStack.size > 1) backStack.removeLastOrNull()
    }

    private fun replaceAll(scene: RootScene) = Snapshot.withMutableSnapshot {
        backStack.clear()
        backStack.add(scene)
    }
}

/**
 * The top-level screens of the application.
 */
@Serializable
sealed interface RootScene : NavKey {

    @Serializable
    data object Start : RootScene

    /**
     * Scene for pending authentication actions.
     *
     * @property url The URL where the user is not authenticated.
     * @property methods List of methods that can be used to authenticate the user again.
     */
    @Serializable
    data class Authentication(val url: Url, val methods: List<AuthMethod>) : RootScene

    /**
     * Scene for users that have access.
     *
     * @property authOption The option that was used to authenticate the user.
     */
    @Serializable
    data class Main(val authOption: AuthOption) : RootScene
}
