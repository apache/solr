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
import org.apache.solr.ui.components.configsets.data.ListConfigSets
import org.apache.solr.ui.components.configsets.overview.OverviewComponent
import org.apache.solr.ui.components.main.MainComponent.Child

/**
 * Contract for the Configsets feature.
 *
 * Responsibilities:
 * - Owns tab/navigation state within the Configsets screen.
 * - Loads and exposes the list of available configsets.
 * - Persists the currently selected tab and configset.
 *
 * Exposed state:
 * - [model]: reactive UI model (configset names, selected tab, selected configset).
 *
 * Actions:
 * - [onSelectTab]: update the active tab.
 * - [onSelectConfigset]: change the active configset.
 *
 * Notes:
 * - Scope: feature-local only; app-level navigation is owned by [MainComponent].
 * - State restoration is implementation-defined (e.g., MVIKotlin Store / Decompose StateKeeper).
 *
 * See also:
 * - Decompose (component lifecycle & composition)
 * - MVIKotlin (unidirectional data flow)
 */
interface ConfigsetsComponent {

    /**
     * Child stack that holds the navigation state.
     */
    val childStack: Value<ChildStack<*, Child>>

    /**
     * All possible navigation targets (children) within the Configsets feature.
     *
     * Each child wraps its own component, which holds the state and logic for that screen.
     */
    sealed interface Child {
        data class Overview(val component: OverviewComponent) : Child
    }

    /**
     * UI model for the Configsets screen.
     *
     * @property selectedTab Zero-based index of the active tab.
     * @property configSets List of available configsets (domain model).
     * @property selectedConfigset Name of the active configset (non-null when available).
     */
    data class Model(
        val selectedTab: Int,
        val configSets: ListConfigSets,
        val selectedConfigset: String = "",
    )

    /** Hot, observable stream of [Model] for Compose/UI. */
    val model: StateFlow<Model>

    /** Select the active tab by index. */
    fun onSelectTab(index: Int)

    /** Select the active configset by name. */
    fun onSelectConfigset(name: String)
}
