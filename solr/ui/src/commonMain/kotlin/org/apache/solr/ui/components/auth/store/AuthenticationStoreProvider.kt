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
import io.ktor.http.Url
import kotlin.coroutines.CoroutineContext
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Label
import org.apache.solr.ui.components.auth.store.AuthenticationStore.State
import org.apache.solr.ui.domain.AuthMethod

/**
 * The store provider that holds the business logic of the [AuthenticationStore].
 *
 * The main role of this store provider is to hold and manage a shared authentication state that can
 * be used across all authentication options available to the user. This state can contain a shared
 * error state, an "authenticating" state and various other data that need to be in sync if multiple
 * authentication options are available at the same time.
 *
 * @property storeFactory The store factory used for creating new stores.
 * @property mainContext Coroutine context where main thread tasks are executed.
 * @property ioContext Coroutine context for input/output (network transaction).
 * @property url The URL of the Solr instance the user is authenticating against.
 * @property methods A list of authentications methods that are supported.
 */
class AuthenticationStoreProvider(
    private val storeFactory: StoreFactory,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val url: Url,
    private val methods: List<AuthMethod>,
) {

    fun provide(): AuthenticationStore = object :
        AuthenticationStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "BasicAuthStore",
            initialState = State(url = url, methods = methods),
            bootstrapper = SimpleBootstrapper(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {
        /**
         * Message that is dispatched when an authentication process is started.
         */
        data object AuthenticationStarted : Message

        /**
         * Message that is dispatched when an authentication error occurred.
         */
        data class AuthenticationFailed(val error: Throwable) : Message

        /**
         * Message that is dispatched when the errors are reset.
         */
        data object ErrorReset : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) = when (intent) {
            is Intent.StartAuthenticating -> dispatch(Message.AuthenticationStarted)
            is Intent.FailAuthentication -> dispatch(Message.AuthenticationFailed(intent.error))
            is Intent.ResetError -> dispatch(Message.ErrorReset)
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.AuthenticationStarted -> copy(isAuthenticating = true)
            is Message.AuthenticationFailed -> copy(error = msg.error, isAuthenticating = false)
            Message.ErrorReset -> copy(error = null)
        }
    }
}
