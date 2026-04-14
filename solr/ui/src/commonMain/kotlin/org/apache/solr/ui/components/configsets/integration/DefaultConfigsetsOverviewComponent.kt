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

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.router.slot.SlotNavigation
import com.arkivanov.decompose.router.slot.activate
import com.arkivanov.decompose.router.slot.childSlot
import com.arkivanov.decompose.router.slot.dismiss
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent
import org.apache.solr.ui.utils.AppComponentContext

internal class DefaultConfigsetsOverviewComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    createConfigset: (AppComponentContext, (CreateConfigsetComponent.Output) -> Unit) -> CreateConfigsetComponent,
    importConfigset: (AppComponentContext, (ImportConfigsetComponent.Output) -> Unit) -> ImportConfigsetComponent,
    private val configsetsComponent: ConfigsetsComponent? = null,
) : ConfigsetsOverviewComponent,
    AppComponentContext by componentContext {

    constructor(
        componentContext: AppComponentContext,
        configsetsComponent: ConfigsetsComponent,
        storeFactory: StoreFactory,
        httpClient: HttpClient,
    ) : this(
        componentContext = componentContext,
        configsetsComponent = configsetsComponent,
        storeFactory = storeFactory,
        httpClient = httpClient,
        createConfigset = { childContext, output ->
            DefaultCreateConfigsetComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                configsetsComponent = configsetsComponent,
                output = output,
            )
        },
        importConfigset = { childContext, output ->
            DefaultImportConfigsetComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                output = output,
            )
        },
    )

    private val dialogNavigation =
        SlotNavigation<ConfigsetsOverviewComponent.CreateConfigsetDialogConfig>()

    override val dialog: Value<ChildSlot<ConfigsetsOverviewComponent.CreateConfigsetDialogConfig, *>> =
        childSlot(
            source = dialogNavigation,
            key = "CreateConfigsetDialog",
            serializer = ConfigsetsOverviewComponent.CreateConfigsetDialogConfig.serializer(),
            handleBackButton = true,
        ) { config, childComponentContext ->
            when (config) {
                is ConfigsetsOverviewComponent.CreateConfigsetDialogConfig.CreateConfigsetWithInputDialogConfig ->
                    createConfigset(childComponentContext, ::createOutput)

                is ConfigsetsOverviewComponent.CreateConfigsetDialogConfig.ImportConfigsetDialogConfig ->
                    importConfigset(childComponentContext, ::importOutput)
            }
        }

    override fun createConfigset() = dialogNavigation.activate(
        ConfigsetsOverviewComponent.CreateConfigsetDialogConfig.CreateConfigsetWithInputDialogConfig,
    )

    override fun importConfigset() = dialogNavigation.activate(
        ConfigsetsOverviewComponent.CreateConfigsetDialogConfig.ImportConfigsetDialogConfig,
    )

    override fun closeDialog() = dialogNavigation.dismiss()

    override fun editSolrConfig(name: String) {
        TODO("Not yet implemented")
    }

    private fun createOutput(output: CreateConfigsetComponent.Output) = when (output) {
        is CreateConfigsetComponent.Output.ConfigsetCreated -> {
            dialogNavigation.dismiss()
            configsetsComponent?.onSelectConfigset(
                name = output.configset.name,
                reload = true,
            )
        }
    }

    private fun importOutput(output: ImportConfigsetComponent.Output) = when (output) {
        is ImportConfigsetComponent.Output.ConfigsetImported -> {
            dialogNavigation.dismiss()
            configsetsComponent?.onSelectConfigset(
                name = output.configset.name,
                reload = true,
            )
        }
    }
}
