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

package org.apache.solr.ui.views.navigation

import androidx.compose.material3.PrimaryScrollableTabRow
import androidx.compose.material3.Tab
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import kotlin.enums.EnumEntries
import org.apache.solr.ui.components.navigation.TabNavigationComponent
import org.jetbrains.compose.resources.StringResource
import org.jetbrains.compose.resources.stringResource

/**
 * Navigation tabs that can be used for adding basic navigation elements to a section.
 *
 * @param component The tab navigation component that manages the tab navigation.
 * @param entries The entries to display in the navigation.
 * @param mapper Mapper function that maps the entries to string resources, used to render the tab
 * labels.
 * @param modifier Modifier to apply to the root element.
 */
@Composable
fun <T : Enum<T>, C : Any> NavigationTabs(
    component: TabNavigationComponent<T, C>,
    entries: EnumEntries<T>, // TODO See if we can skip this parameter
    mapper: (T) -> StringResource,
    modifier: Modifier = Modifier,
) {
    val slot by component.tabSlot.subscribeAsState()

    val currentTab = slot.child?.configuration
    val currentTabIndex = currentTab?.ordinal ?: 0

    PrimaryScrollableTabRow(
        modifier = modifier,
        selectedTabIndex = currentTabIndex,
        edgePadding = 16.dp,
    ) {
        entries.forEach { tab ->
            val selected = currentTab == tab

            Tab(
                selected = selected,
                onClick = { component.onNavigate(tab) },
                text = {
                    Text(
                        text = stringResource(mapper(tab)),
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                },
            )
        }
    }
}
