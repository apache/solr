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

package org.apache.solr.ui.components.files.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.files.store.FilePickerStore.Intent
import org.apache.solr.ui.components.files.store.FilePickerStore.Label
import org.apache.solr.ui.components.files.store.FilePickerStore.State
import org.apache.solr.ui.domain.PickedFile
import org.apache.solr.ui.utils.pickFile

internal class FilePickerStoreProvider(
    private val storeFactory: StoreFactory,
    private val mainContext: CoroutineContext,
    private val ioContext: CoroutineContext,
) {

    fun provide(): FilePickerStore = object :
        FilePickerStore,
        Store<Intent, State, Label> by storeFactory.create(
            name = "FilerPickerStore",
            initialState = State(),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Message {

        data class FileSelected(val file: PickedFile) : Message

        data object FileSelectionAborted : Message

        data object SelectionCleared : Message

        data object SelectingFile : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Unit, State, Message, Label>(mainContext) {
        override fun executeIntent(intent: Intent) = when (intent) {
            Intent.ClearSelection -> dispatch(Message.SelectionCleared)
            Intent.OpenFilePicker -> openFilePicker()
        }

        private fun openFilePicker() {
            dispatch(Message.SelectingFile)
            // Launch in main scope as it is tightly coupled with user-interaction
            scope.launch {
                val pickedFile = pickFile(extensions = listOf("zip"))

                if (pickedFile == null) {
                    dispatch(Message.FileSelectionAborted)
                    return@launch
                }

                publish(Label.FilePicked(pickedFile))
                dispatch(Message.FileSelected(pickedFile))
            }
        }
    }

    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.FileSelected -> State(selectedFile = msg.file)
            is Message.SelectingFile -> copy(isFileSelectionEnabled = false)
            is Message.SelectionCleared -> State()
            is Message.FileSelectionAborted -> this
        }
    }
}
