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
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Label
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.State
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.domain.PickedFile

internal class ImportConfigsetStoreProvider(
    private val storeFactory: StoreFactory,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
    private val client: Client,
) {

    fun provide(): ImportConfigsetStore = object :
        ImportConfigsetStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "ImportConfigsetStore",
            initialState = State(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {

        data class ConfigsetNameChanged(val configsetName: String) : Message

        data class UploadingConfigset(val progress: Float) : Message

        data object ImportingConfigset : Message

        data class ConfigsetImported(val configset: Configset) : Message

        data class ConfigsetImportFailed(val errors: List<Throwable>) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) = when (intent) {
            is Intent.ChangeConfigsetName -> dispatch(Message.ConfigsetNameChanged(intent.configsetName))
            is Intent.ImportConfigset -> importConfigset(pickedFile = intent.file)
        }

        private fun importConfigset(pickedFile: PickedFile) {
            dispatch(Message.ImportingConfigset)
            val name = state().name.ifBlank { pickedFile.name }
            scope.launch {
                withContext(ioContext) {
                    client.importConfigset(name, pickedFile)
                }.onSuccess {
                    dispatch(Message.ConfigsetImported(it))
                    publish(Label.ConfigsetImported(it))
                }
            }
        }
    }

    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.ConfigsetNameChanged -> copy(
                name = msg.configsetName,
                isNameEdited = true,
            )
            is Message.ConfigsetImportFailed -> copy(isLoading = false) // TODO Pass error to UI
            is Message.ConfigsetImported -> copy(isLoading = false)
            is Message.ImportingConfigset -> copy(isLoading = true)
            is Message.UploadingConfigset -> copy(isLoading = true)
        }
    }

    interface Client {

        /**
         * Creates a new configset with the specified [name] by importing / uploading the [file].
         *
         * @param name Name of the configset to create.
         * @param file The file to upload.
         */
        suspend fun importConfigset(name: String, file: PickedFile): Result<Configset>
    }
}
