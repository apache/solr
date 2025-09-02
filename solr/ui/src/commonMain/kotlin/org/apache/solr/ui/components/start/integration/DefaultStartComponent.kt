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

package org.apache.solr.ui.components.start.integration

import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.store.StartStore
import org.apache.solr.ui.components.start.store.StartStore.Intent
import org.apache.solr.ui.components.start.store.StartStoreProvider
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultStartComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    output: (StartComponent.Output) -> Unit,
) : StartComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        StartStoreProvider(
            storeFactory = storeFactory,
            client = HttpStartStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, startStateToModel)

    init {
        store.labels.onEach { label ->
            when (label) {
                is StartStore.Label.AuthRequired -> output(
                    StartComponent.Output.OnAuthRequired(
                        url = label.url,
                        methods = label.methods,
                    ),
                )
                is StartStore.Label.Connected ->
                    output(StartComponent.Output.OnConnected(url = label.url))
            }
        }.launchIn(mainScope)
    }

    override fun onSolrUrlChange(url: String) = store.accept(Intent.UpdateSolrUrl(url))

    override fun onConnect() = store.accept(Intent.Connect)
}
