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

import ConfigsetsNavBar
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
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Child
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

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
    val model by component.model.collectAsState()

    // Decompose child to access OverviewComponent
    val stack by component.childStack.subscribeAsState()
    val currentChild = stack.active.instance

    val renderTab: @Composable (ConfigsetsTab, String) -> Unit = { tab, _ ->
        when (tab) {
            ConfigsetsTab.Overview -> when (val child = currentChild) {
                is Child.Overview -> ConfigsetsOverviewContent(component = child.component)
                else -> Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) { Text("Overview unavailable") }
            }
            else -> Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) { Text(tab.name) }
        }
    }

    Column(Modifier.fillMaxSize()) {
        ConfigsetsNavBar(
            selectedTab = model.selectedTab,
            selectTab = { tab: ConfigsetsTab -> component.onSelectTab(tab) },
            availableConfigsets = model.configsets,
        )
        ConfigsetsDropdown(
            selectedConfigSet = model.selectedConfigset,
            selectConfigset = { s: String -> component.onSelectConfigset(s) },
            availableConfigsets = model.configsets,
        )

        Box(Modifier.fillMaxSize().padding(16.dp)) {
            renderTab(model.selectedTab, model.selectedConfigset)
        }
    }
}
