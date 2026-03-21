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
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore.Intent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore.State

/**
 * Store provider that [provide]s instances of [ConfigsetsStore].
 *
 * @property storeFactory Store factory to use for creating the store.
 * @property client Client implementation to use for resolving [Intent]s and [Action]s.
 * @property ioContext Coroutine context used for IO activity.
 */
internal class ConfigsetsStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {

    fun provide(): ConfigsetsStore = object :
        ConfigsetsStore,
        Store<Intent, State, Nothing> by storeFactory.create(
            name = "ConfigsetsStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(Action.FetchInitialConfigsets),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Action {
        /**
         * Action used for initiating the initial fetch of configsets data.
         */
        data object FetchInitialConfigsets : Action
    }

    private sealed interface Message {
        data class ConfigSetsUpdated(val configsets: ListConfigsets) : Message
        data class SelectedConfigSetChanged(val configsetName: String) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, Nothing>(mainContext) {
        override fun executeAction(action: Action) = when (action) {
            Action.FetchInitialConfigsets -> {
                fetchConfigsets()
            }
        }

        override fun executeIntent(intent: Intent) = when (intent) {
            is Intent.SelectConfigset -> {
                if (intent.reload) {
                    fetchConfigsets {
                        dispatch(Message.SelectedConfigSetChanged(intent.configSetName))
                    }
                } else {
                    dispatch(Message.SelectedConfigSetChanged(intent.configSetName))
                }
            }
        }

        private fun fetchConfigsets(callback: () -> Unit = {}) {
            scope.launch {
                withContext(ioContext) {
                    client.fetchConfigSets()
                }.onSuccess { sets ->
                    dispatch(Message.ConfigSetsUpdated(sets))
                    callback()
                }
            }
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.ConfigSetsUpdated -> copy(configSets = msg.configsets)
            is Message.SelectedConfigSetChanged -> copy(selectedConfigset = msg.configsetName)
        }
    }

    /**
     * Client interface for fetching configsets information.
     */
    interface Client {
        /** To fetch a list of configsets. */
        suspend fun fetchConfigSets(): Result<ListConfigsets>

        suspend fun deleteConfigset(configsetName: String): Result<Unit>
    }
}
