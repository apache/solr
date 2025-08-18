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
import androidx.compose.ui.Modifier
import com.arkivanov.decompose.extensions.compose.stack.Children
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.views.auth.UserAuthenticationContent
import org.apache.solr.ui.views.main.MainContent
import org.apache.solr.ui.views.navigation.Footer
import org.apache.solr.ui.views.start.StartContent

/**
 * The root composable of the Compose application. This function is used as the shared entry
 * point of all targets.
 *
 * @param component Component that manages the state of the root composable.
 */
@Composable
fun RootContent(
    component: RootComponent,
    modifier: Modifier = Modifier,
) {
    Column(modifier = modifier.fillMaxSize()) {
        Children(
            stack = component.childStack,
            modifier = Modifier.weight(1f),
        ) {
            when (val child = it.instance) {
                is RootComponent.Child.Start -> StartContent(
                    component = child.component,
                    modifier = Modifier.fillMaxSize(),
                )

                is RootComponent.Child.Main -> MainContent(
                    component = child.component,
                    modifier = Modifier.fillMaxSize(),
                )

                is RootComponent.Child.Authentication -> UserAuthenticationContent(
                    component = child.component,
                    modifier = Modifier.fillMaxSize(),
                )
            }
        }
        Footer(modifier = Modifier.fillMaxWidth())
    }
}
