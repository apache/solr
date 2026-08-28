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

import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.scene.DialogSceneStrategy
import androidx.navigation3.scene.DialogSceneStrategy.Companion.dialog
import androidx.navigation3.scene.SinglePaneSceneStrategy
import androidx.navigation3.ui.NavDisplay
import kotlin.collections.removeLastOrNull
import org.apache.solr.ui.components.configsets.di.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetEvent
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewEntry
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewEntry.ConfigsetsOverviewDialog.CreateConfigsetDialog
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewEntry.ConfigsetsOverviewDialog.ImportConfigsetDialog
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

    val configsetsModel by configsetsViewModel.uiState.collectAsState()

    val onConfigsetCreated: (Configset) -> Unit = {
        viewModel.closeDialog()
        configsetsViewModel.reloadConfigsets()
        configsetsViewModel.selectConfigset(it.name)
    }

    val createConfigsetEventCollector: (CreateConfigsetEvent) -> Unit = { event ->
        when (event) {
            is CreateConfigsetEvent.ConfigsetCreated -> onConfigsetCreated(event.configset)

            is CreateConfigsetEvent.ConfigsetCreationAborted -> viewModel.closeDialog()

            is CreateConfigsetEvent.ConfigsetCreateToggleInputForm ->
                if (event.useFileInput) {
                    viewModel.openImportConfigsetDialog()
                } else {
                    viewModel.openCreateConfigsetDialog()
                }

            else -> Unit
        }
    }

    NavDisplay(
        modifier = Modifier.horizontalScroll(rememberScrollState()),
        backStack = viewModel.backStack,
        sceneStrategies = listOf(DialogSceneStrategy(), SinglePaneSceneStrategy()),
        onBack = {
            if (viewModel.backStack.last() is ConfigsetsOverviewEntry.ConfigsetsOverviewDialog) {
                viewModel.backStack.removeLastOrNull()
            }
        },
        entryDecorators = listOf(rememberViewModelStoreNavEntryDecorator()),
        entryProvider = entryProvider {
            entry<ConfigsetsOverviewEntry.Main> {
                OverviewContent(
                    viewModel = configsetsViewModel,
                    selectedConfigset = configsetsModel.selectedConfigset,
                    onOpenCreateConfigsetDialog = viewModel::openCreateConfigsetDialog,
                    onEditSolrConfig = viewModel::editSolrConfig,
                )
            }
            entry<CreateConfigsetDialog>(metadata = dialog()) {
                val createViewModel = viewModel { component.createCreateConfigsetViewModel() }

                LaunchedEffect(createViewModel) {
                    createViewModel.events.collect(collector = createConfigsetEventCollector)
                }

                CreateConfigsetDialog(viewModel = createViewModel)
            }
            entry<ImportConfigsetDialog>(metadata = dialog()) {
                val importViewModel = viewModel { component.createImportConfigsetViewModel() }

                LaunchedEffect(importViewModel) {
                    importViewModel.events.collect(collector = createConfigsetEventCollector)
                }

                ImportConfigsetDialog(viewModel = importViewModel)
            }
        },
    )
}

@Composable
private fun OverviewContent(
    viewModel: ConfigsetsViewModel,
    onOpenCreateConfigsetDialog: () -> Unit,
    onEditSolrConfig: (String) -> Unit,
    modifier: Modifier = Modifier,
    selectedConfigset: String? = null,
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(8.dp),
    verticalAlignment = Alignment.CenterVertically,
) {
    ConfigsetsDropdown(
        viewModel = viewModel,
        modifier = Modifier.widthIn(min = 128.dp, max = 256.dp),
    )

    SolrTextButton(onClick = onOpenCreateConfigsetDialog) {
        Icon(painter = painterResource(Res.drawable.add), contentDescription = null)
        Spacer(modifier = Modifier.width(8.dp))
        Text(stringResource(Res.string.action_create_configset))
    }
    if (!selectedConfigset.isNullOrBlank()) {
        SolrTextButton(onClick = { onEditSolrConfig(selectedConfigset) }) {
            Icon(painter = painterResource(Res.drawable.edit), contentDescription = null)
            Spacer(modifier = Modifier.width(8.dp))
            Text(stringResource(Res.string.action_edit_solrconfig))
        }
    }
}
