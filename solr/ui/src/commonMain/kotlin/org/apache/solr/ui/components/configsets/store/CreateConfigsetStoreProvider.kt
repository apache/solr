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

package org.apache.solr.ui.components.configsets.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.Label
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.State
import org.apache.solr.ui.domain.Configset

internal class CreateConfigsetStoreProvider(
    private val storeFactory: StoreFactory,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val client: Client,
    private val configsets: List<Configset>,
    private val selectedConfigset: String? = null,
) {

    fun provide(): CreateConfigsetStore = object :
        CreateConfigsetStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "CreateConfigsetStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(
                Action.InitializeWithConfigsets(configsets, selectedConfigset),
            ),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Action {
        /**
         * Action for initializing the provided store with a set of base configsets.
         */
        data class InitializeWithConfigsets(val configsets: List<Configset>, val selectedConfigset: String?) : Action
    }

    private sealed interface Message {

        data class ConfigsetNameUpdated(val configsetName: String) : Message

        data class SelectedBaseConfigsetChanged(val baseConfigset: String?) : Message

        data class ConfigsetsUpdated(val configsets: List<Configset>) : Message

        data object CreatingConfigset : Message

        data class ConfigsetCreated(val configset: Configset) : Message

        data class ConfigsetCreationFailed(val errors: List<Throwable>) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, Label>(mainContext) {
        override fun executeAction(action: Action) = when (action) {
            is Action.InitializeWithConfigsets -> {
                dispatch(Message.ConfigsetsUpdated(action.configsets))
                action.selectedConfigset?.let {
                    dispatch(Message.SelectedBaseConfigsetChanged(baseConfigset = it))
                }
                Unit
            }
        }

        override fun executeIntent(intent: Intent) = when (intent) {
            is Intent.SelectConfigsetFile ->
                dispatch(Message.ConfigsetNameUpdated(intent.configsetFile))

            is Intent.ChangeConfigsetName ->
                dispatch(Message.ConfigsetNameUpdated(intent.configsetName))

            is Intent.ChangeBaseConfigset ->
                dispatch(Message.SelectedBaseConfigsetChanged(intent.baseConfigset))

            is Intent.CreateConfigset -> createConfigset()
        }

        private fun createConfigset() {
            val state = state()
            dispatch(Message.CreatingConfigset)
            scope.launch {
                withContext(ioContext) {
                    client.createConfigsetByName(state.configsetName, state.baseConfigset)
                }.onSuccess { publish(Label.ConfigsetCreated(it)) }
            }
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [CreateConfigsetStore.State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.ConfigsetsUpdated -> copy(configsets = msg.configsets)
            is Message.SelectedBaseConfigsetChanged -> copy(baseConfigset = msg.baseConfigset)
            is Message.ConfigsetNameUpdated -> copy(configsetName = msg.configsetName)
            is Message.CreatingConfigset -> copy(isLoading = true)
            is Message.ConfigsetCreated -> copy(
                isLoading = false,
                configsetName = "",
                baseConfigset = null,
            )
            is Message.ConfigsetCreationFailed -> copy(isLoading = false)
        }
    }

    interface Client {
        suspend fun createConfigsetByName(
            configsetName: String,
            baseConfigset: String? = null,
        ): Result<Configset>
    }
}
