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

package org.apache.solr.ui.components.start.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import org.apache.solr.ui.components.start.store.StartStore.Intent
import org.apache.solr.ui.components.start.store.StartStore.State

/**
 * Store provider that [provide]s instances of [StartStore].
 *
 * @property storeFactory Store factory to use for creating the store.
 * @property client Client implementation to use for resolving [Intent]s and [Action]s.
 * @property ioContext Coroutine context used for IO activity.
 */
internal class StartStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val ioContext: CoroutineContext,
) {

    fun provide(): StartStore = object :
        StartStore,
        Store<Intent, State, Nothing> by storeFactory.create(
            name = "StartStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {

        /**
         * Message that is dispatched when the stored Solr URL is updated.
         *
         * @property url The new Solr URL.
         */
        data class UrlUpdated(val url: String) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Nothing>() {
        override fun executeIntent(intent: Intent) {
            when (intent) {
                is Intent.UpdateSolrUrl -> dispatch(Message.UrlUpdated(intent.url))
                is Intent.Connect -> {
                    // TODO Initiate connect
                }
            }
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.UrlUpdated -> copy(url = msg.url)
        }
    }

    /**
     * Client interface for fetching environment information.
     */
    interface Client {

        /**
         * Tries to connect to a Solr instance with the given URL.
         *
         * @param url The Solr URL to connect to.
         * @return Result of whether the connection was established.
         */
        suspend fun connect(url: String): Result<Unit>
    }
}
