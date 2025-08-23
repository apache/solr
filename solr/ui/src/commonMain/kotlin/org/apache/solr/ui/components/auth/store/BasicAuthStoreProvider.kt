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
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Intent
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Label
import org.apache.solr.ui.components.auth.store.BasicAuthStore.State
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.utils.parseError

class BasicAuthStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val method: AuthMethod.BasicAuthMethod,
) {

    fun provide(): BasicAuthStore = object :
        BasicAuthStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "BasicAuthStore",
            initialState = State(method = method),
            bootstrapper = SimpleBootstrapper(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {

        /**
         * Message that is dispatched when the username was successfully updated.
         */
        data class UsernameUpdated(val username: String) : Message

        /**
         * Message that is dispatched when the password was successfully updated.
         */
        data class PasswordUpdated(val password: String) : Message

        /**
         * Message that is dispatched when an authentication error occurred.
         */
        data class AuthenticationFailed(val error: Throwable) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) {
            when (intent) {
                is Intent.Authenticate -> authenticate()
                is Intent.UpdateUsername -> {
                    state().error?.let { publish(Label.ErrorReset) }
                    dispatch(Message.UsernameUpdated(intent.username))
                }
                is Intent.UpdatePassword -> {
                    state().error?.let { publish(Label.ErrorReset) }
                    dispatch(Message.PasswordUpdated(intent.password))
                }
            }
        }

        private fun authenticate(): Unit = with(state()) {
            publish(Label.AuthenticationStarted)
            scope.launch(
                context = CoroutineExceptionHandler { _, throwable ->
                    // error returned here is platform-specific and needs further parsing
                    publish(Label.AuthenticationFailed(error = parseError(throwable)))
                    dispatch(Message.AuthenticationFailed(error = parseError(throwable)))
                },
            ) {
                withContext(ioContext) {
                    client.authenticate(username, password)
                }.onSuccess {
                    // Authentication succeeded with the given credentials
                    publish(Label.Authenticated(method, username, password))
                }.onFailure { error ->
                    handleConnectionError(error)
                }
            }
        }

        private fun handleConnectionError(error: Throwable) {
            val mappedError: Throwable = when (error) {
                // Unauthorized response means that the credentials are invalid
                is UnauthorizedException -> InvalidCredentialsException()
                else -> error
            }

            dispatch(Message.AuthenticationFailed(mappedError))
            publish(Label.AuthenticationFailed(mappedError))
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.UsernameUpdated -> copy(username = msg.username, error = null)
            is Message.PasswordUpdated -> copy(password = msg.password, error = null)
            is Message.AuthenticationFailed -> copy(error = msg.error)
        }
    }

    interface Client {

        /**
         * Authenticates the user with the current Solr instance.
         *
         * @param username The username to use.
         * @param password The password to use.
         * @return Returns success results ifF the credentials authenticated the user.
         */
        suspend fun authenticate(username: String, password: String): Result<Unit>
    }
}
