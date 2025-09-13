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

package org.apache.solr.ui.preview.configsets

import androidx.compose.runtime.Composable
import com.arkivanov.decompose.Child
import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.MutableValue
import com.arkivanov.decompose.value.Value
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Model
import org.apache.solr.ui.components.configsets.data.Configset
import org.apache.solr.ui.components.configsets.overview.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.navigation.TabNavigationComponent.Configuration
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.configsets.ConfigsetsContent
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab
import org.jetbrains.compose.ui.tooling.preview.Preview

@Preview
@Composable
private fun PreviewConfigsetsContentEmptyConfigsets() = PreviewContainer {
    ConfigsetsContent(component = SimplePreviewConfigsetsComponent())
}

@Preview
@Composable
private fun PreviewConfigsetsContentWithConfigsetSelected() = PreviewContainer {
    val configset = "techproducts"
    ConfigsetsContent(
        component = SimplePreviewConfigsetsComponent(
            model = Model(
                configsets = listOf(configset, "getting_started").map { Configset(name = it) },
                selectedConfigset = configset,
                expanded = false,
            ),
        ),
    )
}

@Preview
@Composable
private fun PreviewConfigsetsContentWithMenuExpanded() = PreviewContainer {
    val configset = "techproducts"
    ConfigsetsContent(
        component = SimplePreviewConfigsetsComponent(
            model = Model(
                configsets = listOf(configset, "getting_started").map { Configset(name = it) },
                selectedConfigset = configset,
                expanded = true,
            ),
        ),
    )
}

private class SimplePreviewConfigsetsComponent(model: Model = Model()) : ConfigsetsComponent {
    override val model: StateFlow<Model> = MutableStateFlow(model)

    override fun onSelectConfigset(name: String) = Unit

    override fun setMenuExpanded(expanded: Boolean) = Unit

    override val tabSlot: Value<ChildSlot<Configuration<ConfigsetsTab>, ConfigsetsComponent.Child>>
        get() = MutableValue(
            initialValue = ChildSlot(
                Child.Created(
                    configuration = Configuration(tab = ConfigsetsTab.Overview),
                    instance = ConfigsetsComponent.Child.Overview(PreviewConfigsetsOverviewComponent),
                ),
            ),
        )

    override fun onNavigate(tab: ConfigsetsTab) = Unit
}

private object PreviewConfigsetsOverviewComponent : ConfigsetsOverviewComponent
