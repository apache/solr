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
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent.Child
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.configsets_index_query
import org.apache.solr.ui.generated.resources.configsets_request_handlers
import org.apache.solr.ui.generated.resources.configsets_search_components
import org.apache.solr.ui.generated.resources.configsets_update_configuration
import org.apache.solr.ui.generated.resources.files
import org.apache.solr.ui.generated.resources.overview
import org.apache.solr.ui.generated.resources.schema
import org.apache.solr.ui.views.navigation.NavigationTabs
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ConfigsetsContent(
    component: ConfigsetsRouteComponent,
    modifier: Modifier = Modifier,
) = FlowRow(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val configsetsComponent = component.configsetsComponent
    val model by configsetsComponent.model.collectAsState()
    val slot by component.tabSlot.subscribeAsState()
    val currentChild = slot.child
    val tab = currentChild?.configuration

    Column(Modifier.fillMaxSize()) {
        NavigationTabs(
            component = component,
            entries = ConfigsetsTab.entries,
            mapper = ::tabLabelRes,
            modifier = Modifier.padding(1.dp),
        )
        tab?.let {
            ConfigsetsActionBar(
                variant = currentChild.instance,
                configsets = model.configsets,
                selectedConfigset = model.selectedConfigset,
                onConfigsetChange = configsetsComponent::onSelectConfigset,
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
            )
        }

        Box(
            Modifier
                .fillMaxSize()
                .padding(16.dp),
        ) {
            currentChild?.let {
                when (val child = it.instance) {
                    is Child.Overview -> ConfigsetsOverviewContent(component = child.component)
                    is Child.Placeholder -> Text(text = child.tabName)
                }
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
    ConfigsetsTab.Handlers -> Res.string.configsets_request_handlers
    ConfigsetsTab.SearchComponents -> Res.string.configsets_search_components
}
