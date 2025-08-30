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
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.ScrollableTabRow
import androidx.compose.material3.Tab
import androidx.compose.material3.TabRowDefaults
import androidx.compose.material3.TabRowDefaults.tabIndicatorOffset
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.configsets.data.Configset
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.configsets_index_query
import org.apache.solr.ui.generated.resources.configsets_search_components
import org.apache.solr.ui.generated.resources.configsets_update_configuration
import org.apache.solr.ui.generated.resources.files
import org.apache.solr.ui.generated.resources.overview
import org.apache.solr.ui.generated.resources.schema
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab
import org.jetbrains.compose.resources.stringResource

@Composable
fun ConfigsetsNavBar(
    selectedTab: ConfigsetsTab,
    selectTab: (ConfigsetsTab) -> Unit,
    availableConfigsets: List<Configset>,
    modifier: Modifier = Modifier,
) {
    if (availableConfigsets.isEmpty()) {
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            Text("No configsets available")
        }
        return
    }

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
    }
}

private fun tabLabelRes(item: ConfigsetsTab) = when (item) {
    ConfigsetsTab.Overview -> Res.string.overview
    ConfigsetsTab.Files -> Res.string.files
    ConfigsetsTab.Schema -> Res.string.schema
    ConfigsetsTab.UpdateConfig -> Res.string.configsets_update_configuration
    ConfigsetsTab.IndexQuery -> Res.string.configsets_index_query
    ConfigsetsTab.Handlers -> Res.string.configsets_index_query
    ConfigsetsTab.SearchComponents -> Res.string.configsets_search_components
}
