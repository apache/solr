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

package org.apache.solr.ui.components.auth.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import org.apache.solr.ui.components.auth.store.UnauthenticatedStore.Intent
import org.apache.solr.ui.components.auth.store.UnauthenticatedStore.Label
import org.apache.solr.ui.components.auth.store.UnauthenticatedStore.State
import org.apache.solr.ui.domain.AuthMethod

class UnauthenticatedStoreProvider(
    private val storeFactory: StoreFactory,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val methods: List<AuthMethod>,
) {

    fun provide(): UnauthenticatedStore = object :
        UnauthenticatedStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "BasicAuthStore",
            initialState = State(methods),
            bootstrapper = SimpleBootstrapper(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {
        /**
         * Message that is dispatched when an authentication process is started.
         */
        data object AuthenticationStarted: Message

        /**
         * Message that is dispatched when an authentication error occurred.
         */
        data class AuthenticationFailed(val error: Throwable) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) = when(intent) {
            is Intent.StartAuthenticating -> dispatch(Message.AuthenticationStarted)
            is Intent.FailAuthentication -> dispatch(Message.AuthenticationFailed(intent.error))
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.AuthenticationStarted -> copy(isAuthenticating = true)
            is Message.AuthenticationFailed -> copy(error = msg.error)
        }
    }
}
