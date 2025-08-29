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

package org.apache.solr.ui.components.configsets

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.value.Value
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.configsets.data.Configset
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.overview.ConfigsetsOverviewComponent
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

/** Component contract for Configsets: manages tab state, available configsets, and current selection. */
interface ConfigsetsComponent {

    /**
     * Child stack that holds the navigation state.
     */
    val childStack: Value<ChildStack<*, Child>>

    /**
     * All possible navigation targets (children) within the Configsets feature.
     */
    sealed interface Child {
        data class Overview(val component: ConfigsetsOverviewComponent) : Child
    }

    /**
     * Model that holds the data of the [ConfigsetsComponent].
     */
    data class Model(
        val configsets: List<Configset> = emptyList(),
        val selectedTab: ConfigsetsTab = ConfigsetsTab.Overview,
        val selectedConfigset: String = "",
    )

    /** Hot, observable stream of [Model] for Compose/UI. */
    val model: StateFlow<Model>

    /** Select the active tab by index. */
    fun onSelectTab(tab: ConfigsetsTab)

    /** Select the active configset by name. */
    fun onSelectConfigset(name: String)
}
