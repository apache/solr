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
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import kotlin.enums.EnumEntries
import org.jetbrains.compose.resources.StringResource
import org.jetbrains.compose.resources.stringResource

/**
 * Navigation tabs that can be used for adding basic navigation elements to a section.
 *
 * @param tabs The tabs to display in the navigation.
 * @param selectedTab The currently selected tab.
 * @param onSelectTab Called when the user selects a tab.
 * @param mapper Mapper function that maps the tabs to string resources, used to render the tab
 * labels.
 * @param modifier Modifier to apply to the root element.
 */
@Composable
fun <T : Enum<T>> NavigationTabs(
    tabs: EnumEntries<T>,
    selectedTab: T,
    onSelectTab: (T) -> Unit,
    mapper: (T) -> StringResource,
    modifier: Modifier = Modifier,
) {
    PrimaryScrollableTabRow(
        modifier = modifier,
        selectedTabIndex = selectedTab.ordinal,
        edgePadding = 16.dp,
    ) {
        tabs.forEach { tab ->
            val selected = selectedTab == tab

            Tab(
                selected = selected,
                onClick = { onSelectTab(tab) },
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
