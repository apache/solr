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

private val configsetTabs = listOf(
    "Overview",
    "Libraries",
    "Files",
    "Schema",
    "Update Configuration",
    "Index / Query",
    "Request Handlers / Dispatchers",
    "Search Components",
)

@Composable
fun ConfigsetsNavBarComponent(
    selectedTab: Int,
    selectTab: (Int) -> Unit,
    selectedConfigSet: String,
    selectConfigset: (String) -> Unit,
    availableConfigsets: ListConfigsets,
    modifier: Modifier = Modifier,
    content: @Composable (tab: String, configset: String) -> Unit = { tab, _ ->
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) { Text(tab) }
    },
) {
    if (availableConfigsets.names.isEmpty()) {
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            Text("No configsets available")
        }
        return
    }

    var expanded by remember { mutableStateOf(false) }

    Column(modifier) {
        ScrollableTabRow(
            selectedTabIndex = selectedTab,
            edgePadding = 16.dp,
            divider = { HorizontalDivider(thickness = 1.dp) },
            indicator = { pos ->
                TabRowDefaults.SecondaryIndicator(Modifier.tabIndicatorOffset(pos[selectedTab]))
            },
        ) {
            configsetTabs.forEachIndexed { i, label ->
                Tab(
                    selected = selectedTab == i,
                    onClick = { selectTab(i) },
                    text = { Text(label, maxLines = 1, overflow = TextOverflow.Ellipsis) },
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
            content(configsetTabs[selectedTab], selectedConfigSet)
        }
    }
}
