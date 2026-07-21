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

package org.apache.solr.ui.components.environment.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.SystemData
import org.apache.solr.ui.components.environment.store.EnvironmentStore.Intent
import org.apache.solr.ui.components.environment.store.EnvironmentStore.State

/**
 * Store provider that [provide]s instances of [EnvironmentStore].
 *
 * @property storeFactory Store factory to use for creating the store.
 * @property client Client implementation to use for resolving [Intent]s and [Action]s.
 * @property ioContext Coroutine context used for IO activity.
 */
internal class EnvironmentStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {

    fun provide(): EnvironmentStore = object :
        EnvironmentStore,
        Store<Intent, State, Nothing> by storeFactory.create(
            name = "EnvironmentStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(Action.FetchInitialSystemData),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Action {

        /**
         * Action used for initiating the initial fetch of environment data.
         */
        data object FetchInitialSystemData : Action
    }

    private sealed interface Message {

        /**
         * Message that is dispatched when system data have been retrieved / updated.
         *
         * @property data New system data that have been received.
         */
        data class SystemDataUpdated(val data: SystemData) : Message

        /**
         * Message that is dispatched when java properties have been retrieved / updated.
         *
         * @property properties New list of java properties.
         */
        data class JavaPropertiesUpdated(val properties: List<JavaProperty>) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, Nothing>(mainContext) {

        override fun executeAction(action: Action) = when (action) {
            Action.FetchInitialSystemData -> {
                fetchSystemData()
                fetchJavaProperties()
            }
        }

        override fun executeIntent(intent: Intent) {
            when (intent) {
                Intent.FetchSystemData -> {
                    fetchSystemData()
                    fetchJavaProperties()
                }
            }
        }

        /**
         * Fetches the system data that are part of the environment state.
         *
         * If successful, a [Message.SystemDataUpdated] with the new properties is dispatched.
         */
        private fun fetchSystemData() {
            scope.launch {
                // TODO Add coroutine exception handler
                withContext(ioContext) {
                    client.getSystemData()
                }.onSuccess {
                    dispatch(Message.SystemDataUpdated(it))
                }
                // TODO Add error handling
            }
        }

        /**
         * Fetches the java properties that are part of the environment state.
         *
         * If successful, a [Message.JavaPropertiesUpdated] with the new properties is dispatched.
         */
        private fun fetchJavaProperties() {
            scope.launch {
                // TODO Add coroutine exception handler
                withContext(ioContext) {
                    client.getJavaProperties()
                }.onSuccess {
                    dispatch(Message.JavaPropertiesUpdated(it))
                }
                // TODO Add error handling
            }
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.SystemDataUpdated -> copy(
                mode = msg.data.mode,
                zkHost = msg.data.zkHost,
                solrHome = msg.data.solrHome,
                coreRoot = msg.data.coreRoot,
                lucene = msg.data.lucene,
                jvm = msg.data.jvm,
                security = msg.data.security,
                system = msg.data.system,
                node = msg.data.node,
            )

            is Message.JavaPropertiesUpdated -> copy(
                javaProperties = msg.properties,
            )
        }
    }

    /**
     * Client interface for fetching environment information.
     */
    interface Client {

        /**
         * Fetches a set of system data.
         *
         * @return Result with the system data fetched.
         */
        suspend fun getSystemData(): Result<SystemData>

        /**
         * Fetches the configured java properties.
         *
         * @return Result with a list of [JavaProperty]s.
         */
        suspend fun getJavaProperties(): Result<List<JavaProperty>>
    }
}
