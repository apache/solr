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
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.navigation3.scene.DialogSceneStrategy
import androidx.navigation3.ui.NavDisplay
import org.apache.solr.ui.components.configsets.di.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetDialog
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsTab
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.configsets_index_query
import org.apache.solr.ui.generated.resources.configsets_request_handlers
import org.apache.solr.ui.generated.resources.configsets_search_components
import org.apache.solr.ui.generated.resources.configsets_update_configuration
import org.apache.solr.ui.generated.resources.files
import org.apache.solr.ui.generated.resources.overview
import org.apache.solr.ui.generated.resources.schema
import org.apache.solr.ui.views.configsets.actionbars.ConfigsetsActionBar
import org.apache.solr.ui.views.configsets.actionbars.ConfigsetsOverviewMainActions
import org.apache.solr.ui.views.navigation.NavigationTabs

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ConfigsetsContent(
    component: ConfigsetsComponent,
    modifier: Modifier = Modifier,
) = FlowRow(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val routeViewModel = viewModel { component.createConfigsetsRouteViewModel() }
    val model by routeViewModel.uiState.collectAsState()

    // TODO Consider moving dialogBackstack to routeViewModel
    val dialogBackStack = rememberNavBackStack(ConfigsetDialog.config, ConfigsetDialog.None)
    val dialogStrategy = remember { DialogSceneStrategy<NavKey>() }

    val configsetsViewModel = viewModel { component.createConfigsetsViewModel() }
    val configsetsModel by configsetsViewModel.uiState.collectAsState()

    Column(Modifier.fillMaxSize()) {
        NavigationTabs(
            tabs = ConfigsetsTab.entries,
            selectedTab = model.selectedTab,
            onSelectTab = routeViewModel::selectTab,
            mapper = ::tabLabelRes,
            modifier = Modifier.padding(1.dp),
        )

        ConfigsetsActionBar(
            configsets = configsetsModel.configsets,
            selectedConfigset = configsetsModel.selectedConfigset,
            onConfigsetChange = configsetsViewModel::selectConfigset,
            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
        ) {
            when(model.selectedTab) {
                ConfigsetsTab.Overview -> ConfigsetsOverviewMainActions(
                    onCreateConfigset = { dialogBackStack.add(ConfigsetDialog.CreateConfigsetDialog) },
                    onEditSolrConfig = { /* TODO Not yet implemented */ },
                )
                else -> Unit // TODO Implement other scenes
            }
        }

        Box(
            Modifier
                .fillMaxSize()
                .padding(16.dp),
        ) {
            when (model.selectedTab) {
                ConfigsetsTab.Overview ->
                    ConfigsetsOverviewContent(viewModel = configsetsViewModel)
                else -> Text(text = model.selectedTab.name)
            }
        }

        val onConfigsetCreated: (Configset) -> Unit = {
            dialogBackStack.apply {
                clear()
                add(ConfigsetDialog.None)
            }
            configsetsViewModel.reloadConfigsets()
            configsetsViewModel.selectConfigset(it.name)
        }

        NavDisplay(
            backStack = dialogBackStack,
            sceneStrategies = listOf(dialogStrategy),
            onBack = { dialogBackStack.removeLastOrNull() },
            entryDecorators = listOf(rememberViewModelStoreNavEntryDecorator()),
            entryProvider = entryProvider {
                entry<ConfigsetDialog.None> {}
                entry<ConfigsetDialog.CreateConfigsetDialog> {
                    CreateConfigsetDialog(
                        viewModel = viewModel { component.createCreateConfigsetViewModel() },
                        onImport = {
                            dialogBackStack.apply {
                                removeLastOrNull()
                                add(ConfigsetDialog.ImportConfigsetDialog)
                            }
                        },
                        onCreated = onConfigsetCreated,
                        onDismissRequest = { dialogBackStack.removeLastOrNull() }
                    )
                }
                entry<ConfigsetDialog.ImportConfigsetDialog>(
                    metadata = DialogSceneStrategy.dialog()
                ) {
                    ImportConfigsetDialog(
                        viewModel = viewModel { component.createImportConfigsetViewModel() },
                        onToggle = {
                            dialogBackStack.apply {
                                removeLastOrNull()
                                add(ConfigsetDialog.CreateConfigsetDialog)
                            }
                        },
                        onCreated = onConfigsetCreated,
                        onDismissRequest = { dialogBackStack.removeLastOrNull() },
                    )
                }
            }
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
