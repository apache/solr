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
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.navigation3.scene.DialogSceneStrategy
import androidx.navigation3.ui.NavDisplay
import org.apache.solr.ui.components.configsets.di.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetDialog
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsViewModel
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_create_configset
import org.apache.solr.ui.generated.resources.action_edit_solrconfig
import org.apache.solr.ui.generated.resources.add
import org.apache.solr.ui.generated.resources.edit
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

@Composable
fun ConfigsetsOverviewContent(
    component: ConfigsetsOverviewComponent,
    configsetsViewModel: ConfigsetsViewModel,
    modifier: Modifier = Modifier,
) = Column(modifier) {
    val viewModel = viewModel { component.createConfigsetsOverviewViewModel() }
    val model by viewModel.uiState.collectAsState()

    val configsetsModel by configsetsViewModel.uiState.collectAsState()
    val selectedConfigset = configsetsModel.selectedConfigset

    Row(
        modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        ConfigsetsDropdown(
            viewModel = configsetsViewModel,
            modifier = Modifier.widthIn(min = 128.dp, max = 256.dp),
        )

        SolrTextButton(onClick = viewModel::openCreateConfigsetDialog) {
            Icon(painter = painterResource(Res.drawable.add), contentDescription = null)
            Spacer(modifier = Modifier.width(8.dp))
            Text(stringResource(Res.string.action_create_configset))
        }
        if (!selectedConfigset.isNullOrBlank()) {
            SolrTextButton(onClick = { viewModel.editSolrConfig(selectedConfigset) }) {
                Icon(painter = painterResource(Res.drawable.edit), contentDescription = null)
                Spacer(modifier = Modifier.width(8.dp))
                Text(stringResource(Res.string.action_edit_solrconfig))
            }
        }
    }

    // TODO Consider moving dialogBackstack to overviewViewModel
    val dialogBackStack = rememberNavBackStack(ConfigsetDialog.config, ConfigsetDialog.None)
    val dialogStrategy = remember { DialogSceneStrategy<NavKey>() }

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
