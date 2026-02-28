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
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent.Model
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent.Output
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStoreProvider
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultCreateConfigsetComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    output: (Output) -> Unit,
    selectedConfigSet: String? = null,
    configsets: List<Configset> = emptyList(),
) : CreateConfigsetComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        CreateConfigsetStoreProvider(
            storeFactory = storeFactory,
            client = HttpConfigsetsStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
            configsets = configsets,
            selectedConfigset = selectedConfigSet,
        ).provide()
    }

    constructor(
        componentContext: AppComponentContext,
        configsetsComponent: ConfigsetsComponent,
        storeFactory: StoreFactory,
        httpClient: HttpClient,
        output: (Output) -> Unit,
    ) : this(
        componentContext = componentContext,
        storeFactory = storeFactory,
        httpClient = httpClient,
        configsets = configsetsComponent.model.value.configsets,
        selectedConfigSet = configsetsComponent.model.value.selectedConfigset,
        output = output,
    )

    init {
        store.labels.onEach { label ->
            when (label) {
                is CreateConfigsetStore.Label.ConfigsetCreated ->
                    output(Output.ConfigsetCreated(label.configset))
            }
        }.launchIn(mainScope)
    }

    override fun onConfigsetNameChange(configsetName: String) = store.accept(Intent.ChangeConfigsetName(configsetName))

    override fun onBaseConfigsetChange(baseConfigset: String) = store.accept(Intent.ChangeBaseConfigset(baseConfigset))

    override fun onCreateConfigset() = store.accept(Intent.CreateConfigset)

    override fun onClearBaseConfigset() = store.accept(Intent.ChangeBaseConfigset(null))

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model: StateFlow<Model> =
        store.stateFlow.map(mainScope, createConfigsetStateToModel)
}
