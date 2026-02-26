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
import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.router.slot.SlotNavigation
import com.arkivanov.decompose.router.slot.activate
import com.arkivanov.decompose.router.slot.childSlot
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent.Child
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent.Child.Overview
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent.Child.Placeholder
import org.apache.solr.ui.components.configsets.integration.DefaultConfigsetsOverviewComponent
import org.apache.solr.ui.components.navigation.TabNavigationComponent
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

/**
 * Default implementation of the [ConfigsetsComponent].
 */
internal class DefaultConfigsetsRouteComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    override val configsetsComponent: ConfigsetsComponent = DefaultConfigsetsComponent(
        componentContext = componentContext.childContext("DefaultConfigsetsComponent"),
        storeFactory = storeFactory,
        httpClient = httpClient,
    ),
) : ConfigsetsRouteComponent,
    AppComponentContext by componentContext,
    TabNavigationComponent<ConfigsetsTab, Child> {

    private val navigation = SlotNavigation<ConfigsetsTab>()

    override val tabSlot: Value<ChildSlot<ConfigsetsTab, Child>> = childSlot(
        source = navigation,
        serializer = ConfigsetsTab.serializer(),
        handleBackButton = true,
        childFactory = { configuration, childContext ->
            when (configuration) {
                ConfigsetsTab.Overview -> Overview(
                    component = DefaultConfigsetsOverviewComponent(
                        componentContext = childContext,
                        configsetsComponent = configsetsComponent,
                        storeFactory = storeFactory,
                        httpClient = httpClient,
                    ),
                )
                else -> Placeholder(tabName = configuration.name)
            }
        },
    )

    init {
        navigation.activate(configuration = ConfigsetsTab.Overview)
    }

    override fun onNavigate(tab: ConfigsetsTab) = navigation.activate(configuration = tab)
}
