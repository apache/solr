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
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Intent
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Label
import org.apache.solr.ui.components.auth.store.BasicAuthStore.State
import org.apache.solr.ui.errors.InvalidCredentialsException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.parseError

class BasicAuthStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {

    fun provide(): BasicAuthStore = object :
        BasicAuthStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "BasicAuthStore",
            initialState = State(),
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
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) = when(intent) {
            is Intent.Authenticate -> authenticate()
            is Intent.UpdateUsername -> dispatch(Message.UsernameUpdated(intent.username))
            is Intent.UpdatePassword -> dispatch(Message.PasswordUpdated(intent.password))
        }

        private fun authenticate(): Unit = with(state()) {
            scope.launch(context = CoroutineExceptionHandler { _, throwable ->
                // error returned here is platform-specific and needs further parsing
                publish(Label.AuthenticationFailed(error = parseError(throwable)))
            }) {
                withContext(ioContext) {
                    client.authenticate(username, password)
                }.onSuccess {
                    // Authentication succeeded with the given credentials
                    publish(Label.Authenticated(username, password))
                }.onFailure { error ->
                    handleConnectionError(error)
                }
            }
        }

        private fun handleConnectionError(error: Throwable) = when (error) {
            is UnauthorizedException -> {
                // Unauthorized response means that the credentials are invalid
                publish(Label.AuthenticationFailed(error = InvalidCredentialsException()))
            }
            else -> publish(Label.AuthenticationFailed(error))
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.UsernameUpdated -> copy(username = msg.username, usernameError = null)
            is Message.PasswordUpdated -> copy(password = msg.password, passwordError = null)
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
