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

package org.apache.solr.composeui.ui.root

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.composeui.components.navigation.MainMenu
import org.apache.solr.composeui.components.root.RootComponent
import org.apache.solr.composeui.ui.navigation.Footer
import org.apache.solr.composeui.ui.navigation.NavigationSideBar

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
    // TODO Move selected to component / state store
    var selected by remember { mutableStateOf(MainMenu.Logging) }

    Column(modifier = modifier.fillMaxSize()) {
        Row(modifier = Modifier.weight(1f)) {
            NavigationSideBar(
                modifier = Modifier.fillMaxHeight()
                    .width(224.dp),
                selectedItem = selected,
                onNavigate = { selected = it }, // TODO Add proper onNavigate handler
            )

            // TODO Add page-specific content here
        }
        Footer(modifier = Modifier.fillMaxWidth())
    }
}
