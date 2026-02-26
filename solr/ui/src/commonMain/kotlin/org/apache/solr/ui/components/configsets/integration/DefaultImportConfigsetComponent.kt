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

package org.apache.solr.ui.components.configsets.integration

import com.arkivanov.decompose.childContext
import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent.Output
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Label
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStoreProvider
import org.apache.solr.ui.components.files.FilePickerComponent
import org.apache.solr.ui.components.files.integration.DefaultFilePickerComponent
import org.apache.solr.ui.components.files.store.FilePickerStore
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

internal class DefaultImportConfigsetComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    output: (Output) -> Unit,
) : ImportConfigsetComponent,
    AppComponentContext by componentContext {

    override val filePicker: FilePickerComponent by lazy {
        DefaultFilePickerComponent(
            componentContext = childContext("ImportConfigsetFilePicker"),
            storeFactory = storeFactory,
            output = ::fileOutput,
        )
    }

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        ImportConfigsetStoreProvider(
            storeFactory = storeFactory,
            client = HttpConfigsetsStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    init {
        store.labels.onEach { label ->
            when (label) {
                is Label.ConfigsetImported -> output(Output.ConfigsetImported(label.configset))
            }
        }.launchIn(mainScope)
    }

    override fun onImportConfigset() {
        filePicker.model.value.selectedFile?.let {
            store.accept(Intent.ImportConfigset(file = it))
        }
    }

    override fun onConfigsetNameChange(configsetName: String) = store.accept(Intent.ChangeConfigsetName(configsetName))

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model: StateFlow<ImportConfigsetComponent.Model> =
        store.stateFlow.map(mainScope, importConfigsetStateToModel)

    private fun fileOutput(output: FilePickerComponent.Output) = when (output) {
        is FilePickerComponent.Output.FilePicked -> store.accept(
            Intent.ChangeConfigsetName(
                output.file.name
                    .removeSuffix(".${output.file.extension}")
                    .removeSuffix("_configset"),
            ),
        )
    }
}
