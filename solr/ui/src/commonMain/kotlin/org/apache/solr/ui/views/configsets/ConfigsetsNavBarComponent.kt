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
@file:OptIn(ExperimentalMaterial3Api::class)

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.ScrollableTabRow
import androidx.compose.material3.Tab
import androidx.compose.material3.TabRowDefaults
import androidx.compose.material3.TabRowDefaults.tabIndicatorOffset
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.configsets_files
import org.apache.solr.ui.generated.resources.configsets_index_query
import org.apache.solr.ui.generated.resources.configsets_overview
import org.apache.solr.ui.generated.resources.configsets_request_handlers
import org.apache.solr.ui.generated.resources.configsets_schema
import org.apache.solr.ui.generated.resources.configsets_search_components
import org.apache.solr.ui.generated.resources.configsets_update_configuration
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab
import org.jetbrains.compose.resources.stringResource

@Composable
fun ConfigsetsNavBarComponent(
    selectedTab: ConfigsetsTab,
    selectTab: (ConfigsetsTab) -> Unit,
    selectedConfigSet: String,
    selectConfigset: (String) -> Unit,
    availableConfigsets: ListConfigsets,
    modifier: Modifier = Modifier,
    content: @Composable (tab: ConfigsetsTab, configset: String) -> Unit = { tab, _ ->
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) { Text(stringResource(tabLabelRes(tab))) }
    },
) {
    if (availableConfigsets.names.isEmpty()) {
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            Text("No configsets available")
        }
        return
    }

    var expanded by remember { mutableStateOf(false) }
    val selectedIndex = ConfigsetsTab.entries.indexOf(selectedTab)
    Column(modifier) {
        ScrollableTabRow(
            selectedTabIndex = selectedIndex,
            edgePadding = 16.dp,
            divider = { HorizontalDivider(thickness = 1.dp) },
            indicator = { pos ->
                TabRowDefaults.SecondaryIndicator(
                    Modifier.tabIndicatorOffset(pos[selectedIndex]),
                )
            },
        ) {
            ConfigsetsTab.entries.forEach { tab ->
                Tab(
                    selected = selectedTab == tab,
                    onClick = { selectTab(tab) },
                    text = { Text(stringResource(tabLabelRes(tab)), maxLines = 1, overflow = TextOverflow.Ellipsis) },
                )
            }
        }

        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            ExposedDropdownMenuBox(
                expanded = expanded,
                onExpandedChange = { expanded = !expanded },
                modifier = Modifier.widthIn(min = 256.dp).weight(1f),
            ) {
                OutlinedTextField(
                    value = selectedConfigSet,
                    onValueChange = {},
                    readOnly = true,
                    label = { Text("Configset") },
                    trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded) },
                    modifier = Modifier.menuAnchor().fillMaxWidth(),
                )
                ExposedDropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
                    availableConfigsets.names.forEach { name ->
                        DropdownMenuItem(
                            text = { Text(name) },
                            onClick = {
                                selectConfigset(name)
                                expanded = false
                            },
                        )
                    }
                }
            }
        }

        Box(Modifier.fillMaxSize().padding(16.dp)) {
            content(selectedTab, selectedConfigSet)
        }
    }
}

private fun tabLabelRes(item: ConfigsetsTab) = when (item) {
    ConfigsetsTab.Overview -> Res.string.configsets_overview
    ConfigsetsTab.Files -> Res.string.configsets_files
    ConfigsetsTab.Schema -> Res.string.configsets_schema
    ConfigsetsTab.UpdateConfig -> Res.string.configsets_update_configuration
    ConfigsetsTab.IndexQuery -> Res.string.configsets_index_query
    ConfigsetsTab.Handlers -> Res.string.configsets_request_handlers
    ConfigsetsTab.SearchComponents -> Res.string.configsets_search_components
}
