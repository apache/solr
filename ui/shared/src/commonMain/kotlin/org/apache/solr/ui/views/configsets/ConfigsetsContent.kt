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
package org.apache.solr.ui.views.configsets

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.ui.NavDisplay
import org.apache.solr.ui.components.configsets.di.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsScene
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsTab
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.configsets_index_query
import org.apache.solr.ui.generated.resources.configsets_request_handlers
import org.apache.solr.ui.generated.resources.configsets_search_components
import org.apache.solr.ui.generated.resources.configsets_update_configuration
import org.apache.solr.ui.generated.resources.files
import org.apache.solr.ui.generated.resources.overview
import org.apache.solr.ui.generated.resources.schema
import org.apache.solr.ui.views.navigation.NavigationTabs

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ConfigsetsScene(
    component: ConfigsetsComponent,
    modifier: Modifier = Modifier,
) = FlowRow(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val viewModel = viewModel { component.createConfigsetsRouteViewModel() }
    val model by viewModel.uiState.collectAsState()

    val sharedConfigsetsViewModel = viewModel { component.createConfigsetsViewModel() }

    Column(Modifier.fillMaxSize()) {
        NavigationTabs(
            tabs = ConfigsetsTab.entries,
            selectedTab = model.selectedTab,
            onSelectTab = viewModel::selectTab,
            mapper = ::tabLabelRes,
            modifier = Modifier.padding(1.dp),
        )

        NavDisplay(
            backStack = viewModel.backStack,
            entryDecorators = listOf(rememberViewModelStoreNavEntryDecorator()),
            entryProvider = entryProvider {
                entry<ConfigsetsScene.Overview> {
                    ConfigsetsOverviewContent(
                        component = component.createConfigsetsOverviewComponent(),
                        configsetsViewModel = sharedConfigsetsViewModel,
                        modifier = Modifier.fillMaxSize().padding(16.dp),
                    )
                }
            },
        )
    }
}

private fun tabLabelRes(item: ConfigsetsTab) = when (item) {
    ConfigsetsTab.Overview -> Res.string.overview
    ConfigsetsTab.Files -> Res.string.files
    ConfigsetsTab.Schema -> Res.string.schema
    ConfigsetsTab.UpdateConfig -> Res.string.configsets_update_configuration
    ConfigsetsTab.IndexQuery -> Res.string.configsets_index_query
    ConfigsetsTab.Handlers -> Res.string.configsets_request_handlers
    ConfigsetsTab.SearchComponents -> Res.string.configsets_search_components
}
