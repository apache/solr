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

package org.apache.solr.ui.components.files.integration

import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.apache.solr.ui.components.files.FilePickerComponent
import org.apache.solr.ui.components.files.FilePickerComponent.Output
import org.apache.solr.ui.components.files.store.FilePickerStore
import org.apache.solr.ui.components.files.store.FilePickerStore.Intent
import org.apache.solr.ui.components.files.store.FilePickerStoreProvider
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultFilePickerComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    output: (Output) -> Unit = {},
) : FilePickerComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store: FilePickerStore = instanceKeeper.getStore {
        FilePickerStoreProvider(
            storeFactory = storeFactory,
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    init {
        store.labels.onEach { label ->
            when (label) {
                is FilePickerStore.Label.FilePicked -> output(Output.FilePicked(label.file))
            }
        }.launchIn(mainScope)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model: StateFlow<FilePickerComponent.Model> =
        store.stateFlow.map(mainScope, filePickerStateToModel)

    override fun onSelectFile() = store.accept(Intent.OpenFilePicker)

    override fun clearSelection() = store.accept(Intent.ClearSelection)
}
