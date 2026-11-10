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

package org.apache.solr.ui.components.cluster.viewmodel

import androidx.lifecycle.ViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update

/**
 * View model of the cluster section.
 *
 * The cluster section's goal is to provide a "physical" representation of the connected Solr
 * instance.
 */
class ClusterViewModel : ViewModel() {

    /**
     * UI state of the cluster section.
     */
    val uiState: StateFlow<ClusterUiState>
        field = MutableStateFlow(ClusterUiState())

    /**
     * Switches to the cluster [tab] that was provided.
     *
     * @param tab The tab to select.
     */
    fun selectTab(tab: ClusterTab) = uiState.update { it.copy(selectedTab = tab) }
}

data class ClusterUiState(
    val selectedTab: ClusterTab = ClusterTab.Zookeeper,
)

enum class ClusterTab {
    Zookeeper,
    Nodes,
    Cores,
}
