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

package org.apache.solr.ui.views.root

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.ui.NavDisplay
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.viewmodel.RootScene
import org.apache.solr.ui.views.auth.UserAuthenticationContent
import org.apache.solr.ui.views.main.MainContent
import org.apache.solr.ui.views.navigation.Footer
import org.apache.solr.ui.views.navigation.noPredictiveTransition
import org.apache.solr.ui.views.navigation.noTransition
import org.apache.solr.ui.views.start.StartContent

/**
 * The root composable of the Compose application. This function is used as the shared entry
 * point of all targets.
 *
 * @param component Component that provides the view model of the root composable and the
 * components of the screens.
 */
@Composable
fun RootContent(
    component: RootComponent,
    modifier: Modifier = Modifier,
) {
    val viewModel = viewModel { component.createRootViewModel() }

    Column(modifier = modifier.fillMaxSize()) {
        NavDisplay(
            backStack = viewModel.backStack,
            modifier = Modifier.weight(1f),
            onBack = viewModel::navigateBack,
            entryDecorators = listOf(rememberViewModelStoreNavEntryDecorator()),
            transitionSpec = noTransition(),
            popTransitionSpec = noTransition(),
            predictivePopTransitionSpec = noPredictiveTransition(),
            entryProvider = entryProvider {
                entry<RootScene.Start> {
                    StartContent(
                        component = remember { component.createStartComponent() },
                        onEvent = viewModel::onStartEvent,
                        modifier = Modifier.fillMaxSize(),
                    )
                }

                entry<RootScene.Authentication> { scene ->
                    UserAuthenticationContent(
                        component = remember(scene) {
                            component.createAuthenticationComponent(scene.url, scene.methods)
                        },
                        onEvent = viewModel::onAuthenticationEvent,
                        modifier = Modifier.fillMaxSize(),
                    )
                }

                entry<RootScene.Main> { scene ->
                    MainContent(
                        component = remember(scene) { component.createMainComponent(scene.authOption) },
                        onEvent = viewModel::onMainEvent,
                        modifier = Modifier.fillMaxSize(),
                    )
                }
            },
        )
        Footer(modifier = Modifier.fillMaxWidth())
    }
}
