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
import io.ktor.http.URLParserException
import io.ktor.http.Url
import io.ktor.http.parseUrl
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.start.store.StartStore.Intent
import org.apache.solr.ui.components.start.store.StartStore.Label
import org.apache.solr.ui.components.start.store.StartStore.State
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.utils.DEFAULT_SOLR_URL
import org.apache.solr.ui.utils.parseError

/**
 * Store provider that [provide]s instances of [StartStore].
 *
 * @property storeFactory Store factory to use for creating the store.
 * @property client Client implementation to use for resolving [Intent]s.
 * @property ioContext Coroutine context used for IO activity.
 */
internal class StartStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {

    fun provide(): StartStore = object :
        StartStore,
        Store<Intent, State, Label> by storeFactory.create(
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

        /**
         * Message that is dispatch when an unknown error occurs during connection.
         *
         * @property error Error that was thrown.
         */
        data class ConnectionFailed(val error: Throwable) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) {
            when (intent) {
                is Intent.UpdateSolrUrl -> dispatch(Message.UrlUpdated(intent.url))
                is Intent.Connect -> {
                    var urlValue = state().url
                    if (urlValue == "") urlValue = DEFAULT_SOLR_URL // use placeholder value if empty

                    try {
                        val url = parseUrl(urlValue) ?: throw URLParserException(
                            urlString = urlValue,
                            cause = Error("Invalid URL"),
                        )

                        scope.launch(
                            context = CoroutineExceptionHandler { _, throwable ->
                                // error returned here is platform-specific and needs further parsing
                                dispatch(Message.ConnectionFailed(error = parseError(throwable)))
                            },
                        ) {
                            withContext(ioContext) {
                                client.connect(url)
                            }.onSuccess {
                                // Solr server found with no auth active
                                publish(Label.Connected(url))
                            }.onFailure { error ->
                                handleConnectionError(error, url)
                            }
                        }
                    } catch (error: Exception) {
                        dispatch(Message.ConnectionFailed(error))
                    }
                }
            }
        }

        /**
         * Handles any connection error that may occur during a connection establishment.
         *
         * This function may dispatch [Message]s or publish [Label]s.
         *
         * @param error Error that occurred.
         * @param url The URL that was used for the connection.
         */
        private fun handleConnectionError(error: Throwable, url: Url) = when (error) {
            is UnauthorizedException -> {
                if (error.methods.isEmpty()) {
                    // Solr server found but responded with an unauthorized error
                    // that cannot be processed (supported method or missing information)
                    dispatch(Message.ConnectionFailed(error))
                } else {
                    // Solr server found, but user is unauthorized
                    publish(
                        Label.AuthRequired(
                            url = url,
                            methods = error.methods,
                        ),
                    )
                }
            }
            else -> dispatch(Message.ConnectionFailed(error))
        }
    }

    /**
     * Reducer implementation that consumes [Message]s and updates the store's [State].
     */
    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.UrlUpdated -> copy(url = msg.url, error = null)
            is Message.ConnectionFailed -> copy(error = msg.error)
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
        suspend fun connect(url: Url): Result<Unit>
    }
}
