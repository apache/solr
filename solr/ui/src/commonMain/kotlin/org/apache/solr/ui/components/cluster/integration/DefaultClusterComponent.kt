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

package org.apache.solr.ui.components.cluster.integration

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.router.slot.SlotNavigation
import com.arkivanov.decompose.router.slot.activate
import com.arkivanov.decompose.router.slot.childSlot
import com.arkivanov.decompose.value.Value
import org.apache.solr.ui.components.cluster.ClusterComponent
import org.apache.solr.ui.components.cluster.ClusterComponent.Child
import org.apache.solr.ui.components.cluster.ClusterComponent.ClusterTab
import org.apache.solr.ui.components.navigation.TabNavigationComponent
import org.apache.solr.ui.utils.AppComponentContext

class DefaultClusterComponent(
    componentContext: AppComponentContext,
) : ClusterComponent,
    AppComponentContext by componentContext,
    TabNavigationComponent<ClusterTab, Child> {

    private val navigation = SlotNavigation<ClusterTab>()

    override val tabSlot: Value<ChildSlot<ClusterTab, Child>> = childSlot(
        source = navigation,
        serializer = ClusterTab.serializer(),
        handleBackButton = true,
        childFactory = { configuration, childContext ->
            when (configuration) {
                ClusterTab.Zookeeper -> Child.Zookeeper
                ClusterTab.Nodes -> Child.Nodes
                ClusterTab.Cores -> Child.Cores
            }
        },
    )

    init {
        navigation.activate(configuration = ClusterTab.Zookeeper)
    }

    override fun onNavigate(tab: ClusterTab) = navigation.activate(configuration = tab)
}
