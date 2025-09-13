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

package org.apache.solr.ui.views.cluster

import androidx.compose.foundation.layout.Column
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import org.apache.solr.ui.components.cluster.ClusterComponent
import org.apache.solr.ui.components.cluster.ClusterComponent.ClusterTab
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.cores
import org.apache.solr.ui.generated.resources.nodes
import org.apache.solr.ui.generated.resources.zookeeper
import org.apache.solr.ui.views.navigation.NavigationTabs
import org.jetbrains.compose.resources.StringResource

@Composable
fun ClusterContent(
    component: ClusterComponent,
    modifier: Modifier = Modifier,
) = Column(modifier = modifier) {
    NavigationTabs(
        component = component,
        entries = ClusterTab.entries,
        mapper = ::clusterTabsMapper,
    )
}

private fun clusterTabsMapper(tab: ClusterTab): StringResource = when (tab) {
    ClusterTab.Zookeeper -> Res.string.zookeeper
    ClusterTab.Nodes -> Res.string.nodes
    ClusterTab.Cores -> Res.string.cores
}
