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
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.stack.Children
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.integration.asMainMenu
import org.apache.solr.ui.views.environment.EnvironmentContent
import org.apache.solr.ui.views.logging.LoggingContent
import org.apache.solr.ui.views.navigation.NavigationSideBar

/**
 * The composable used for users that have already authenticated.
 *
 * @param component Component that manages the state of the composable.
 */
@Composable
fun MainContent(
    component: MainComponent,
    modifier: Modifier = Modifier,
) {
    val childStack by component.childStack.subscribeAsState()
    val scrollState = rememberScrollState()

    Row(modifier = modifier) {
        NavigationSideBar(
            modifier = Modifier.fillMaxHeight()
                .width(224.dp),
            selectedItem = childStack.active.instance.asMainMenu,
            onNavigate = component::onNavigate,
            onLogout = component::onLogout,
        )

        Children(
            stack = component.childStack,
            modifier = Modifier.weight(1f),
        ) {
            when (val child = it.instance) {
                is MainComponent.Child.Environment -> EnvironmentContent(
                    component = child.component,
                    modifier = Modifier.fillMaxWidth()
                        .verticalScroll(scrollState)
                        .padding(16.dp),
                )
                is MainComponent.Child.Logging -> LoggingContent(
                    component = child.component,
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        }
    }
}
