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
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Child
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Child.Overview
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Child.Placeholder
import org.apache.solr.ui.components.configsets.overview.integration.DefaultConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore.Intent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStoreProvider
import org.apache.solr.ui.components.navigation.TabNavigationComponent
import org.apache.solr.ui.components.navigation.integration.DefaultTabNavigationComponent
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

/**
 * Default implementation of the [ConfigsetsComponent].
 */
class DefaultConfigsetsComponent internal constructor(
    componentContext: AppComponentContext,
    tabNavigation: TabNavigationComponent<ConfigsetsTab, Child>,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
) : ConfigsetsComponent,
    AppComponentContext by componentContext,
    TabNavigationComponent<ConfigsetsTab, Child> by tabNavigation {

    constructor(
        componentContext: AppComponentContext,
        storeFactory: StoreFactory,
        httpClient: HttpClient,
    ) : this (
        componentContext = componentContext,
        storeFactory = storeFactory,
        httpClient = httpClient,
        tabNavigation = DefaultTabNavigationComponent<ConfigsetsTab, Child>(
            componentContext = componentContext.childContext("ConfigsetsTabs"),
            initialTab = ConfigsetsTab.Overview,
            tabSerializer = ConfigsetsTab.serializer(),
            childFactory = { configuration, childContext ->
                when (configuration.tab) {
                    ConfigsetsTab.Overview -> Overview(
                        DefaultConfigsetsOverviewComponent(
                            componentContext = childContext,
                            storeFactory = storeFactory,
                            httpClient = httpClient,
                        ),
                    )
                    else -> Placeholder(tabName = configuration.tab.name)
                }
            },
        ),
    )

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        ConfigsetsStoreProvider(
            storeFactory = storeFactory,
            client = HttpConfigsetsStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, configsetsStateToModel)

    override fun onSelectConfigset(name: String) {
        store.accept(Intent.SelectConfigSet(configSetName = name))
    }

    override fun setMenuExpanded(expanded: Boolean) {
        store.accept(Intent.SetMenuExpanded(expanded))
    }
}
