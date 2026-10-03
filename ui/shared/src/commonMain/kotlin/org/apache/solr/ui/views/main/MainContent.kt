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

package org.apache.solr.ui.views.main

import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.ui.NavDisplay
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.domain.MainEvent
import org.apache.solr.ui.components.main.viewmodel.MainScene
import org.apache.solr.ui.components.main.viewmodel.menuItem
import org.apache.solr.ui.views.cluster.ClusterContent
import org.apache.solr.ui.views.configsets.ConfigsetsScene
import org.apache.solr.ui.views.environment.EnvironmentContent
import org.apache.solr.ui.views.logging.LoggingContent
import org.apache.solr.ui.views.navigation.NavigationSideBar
import org.apache.solr.ui.views.navigation.noPredictiveTransition
import org.apache.solr.ui.views.navigation.noTransition

/**
 * The composable used for users that have already authenticated.
 *
 * @param component Component that provides the view model of the composable.
 * @param onEvent Called when the main screen emits an event that the parent has to handle,
 * e.g. when the user logs out.
 */
@Composable
fun MainContent(
    component: MainComponent,
    onEvent: (MainEvent) -> Unit,
    modifier: Modifier = Modifier,
) {
    val viewModel = viewModel { component.createMainViewModel() }
    val currentOnEvent by rememberUpdatedState(onEvent)
    val scrollState = rememberScrollState()

    LaunchedEffect(viewModel) {
        viewModel.events.collect { currentOnEvent(it) }
    }

    Row(modifier = modifier) {
        NavigationSideBar(
            modifier = Modifier.fillMaxHeight()
                .width(224.dp),
            selectedItem = viewModel.backStack.last().menuItem,
            onNavigate = viewModel::navigate,
            onLogout = viewModel::logout,
        )

        NavDisplay(
            backStack = viewModel.backStack,
            modifier = Modifier.weight(1f),
            onBack = viewModel::navigateBack,
            entryDecorators = listOf(rememberViewModelStoreNavEntryDecorator()),
            transitionSpec = noTransition(),
            popTransitionSpec = noTransition(),
            predictivePopTransitionSpec = noPredictiveTransition(),
            entryProvider = entryProvider {
                entry<MainScene.Cluster> {
                    ClusterContent(
                        component = component.clusterComponent,
                        modifier = Modifier.fillMaxWidth()
                            .verticalScroll(scrollState),
                    )
                }

                entry<MainScene.Configsets> {
                    ConfigsetsScene(
                        component = component.configsetsComponent,
                        modifier = Modifier.fillMaxWidth()
                            .verticalScroll(scrollState),
                    )
                }

                entry<MainScene.Environment> {
                    EnvironmentContent(
                        component = component.environmentComponent,
                        modifier = Modifier.fillMaxWidth()
                            .verticalScroll(scrollState)
                            .padding(16.dp),
                    )
                }

                entry<MainScene.Logging> {
                    LoggingContent(
                        component = component.loggingComponent,
                        modifier = Modifier.fillMaxWidth(),
                    )
                }
            },
        )
    }
}
