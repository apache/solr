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

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.Logout
import androidx.compose.material.icons.automirrored.rounded.TextSnippet
import androidx.compose.material.icons.automirrored.rounded.ViewList
import androidx.compose.material.icons.rounded.Analytics
import androidx.compose.material.icons.rounded.Apps
import androidx.compose.material.icons.rounded.Dashboard
import androidx.compose.material.icons.rounded.DocumentScanner
import androidx.compose.material.icons.rounded.Folder
import androidx.compose.material.icons.rounded.Hub
import androidx.compose.material.icons.rounded.Memory
import androidx.compose.material.icons.rounded.Security
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Tab
import androidx.compose.material3.Text
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_logout
import org.apache.solr.ui.generated.resources.nav_cluster
import org.apache.solr.ui.generated.resources.nav_collections
import org.apache.solr.ui.generated.resources.nav_configsets
import org.apache.solr.ui.generated.resources.nav_dashboard
import org.apache.solr.ui.generated.resources.nav_environment
import org.apache.solr.ui.generated.resources.nav_logging
import org.apache.solr.ui.generated.resources.nav_metrics
import org.apache.solr.ui.generated.resources.nav_queries_and_operations
import org.apache.solr.ui.generated.resources.nav_security
import org.apache.solr.ui.generated.resources.nav_thread_dump
import org.apache.solr.ui.views.icons.SolrLogo
import org.jetbrains.compose.resources.stringResource

/**
 * The application's main navigation / sidebar. It is used for navigation between the different
 * sections.
 *
 * @param onNavigate Navigation handler function.
 * @param onLogout Logout handler function.
 * @param modifier Modifier to apply to the root composable.
 * @param selectedItem The currently selected navigation item.
 */
@Composable
fun NavigationSideBar(
    onNavigate: (MainMenu) -> Unit,
    onLogout: () -> Unit,
    modifier: Modifier = Modifier,
    selectedItem: MainMenu? = null,
) = Row(modifier = modifier) {
    val scrollState = rememberScrollState(48 * (selectedItem?.ordinal ?: 0))

    Column(Modifier.fillMaxHeight()) {
        SolrLogo(
            modifier = Modifier.fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 8.dp),
        )
        Column(
            modifier = Modifier.weight(1f)
                .verticalScroll(scrollState),
        ) {
            MainMenu.entries.forEach { item ->
                MenuElement(
                    text = stringResource(getMainMenuText(item)),
                    imageVector = getMenuIcon(item),
                    modifier = Modifier.fillMaxWidth(),
                    selected = item == selectedItem,
                    enabled = isSectionAvailable(item),
                    onClick = { onNavigate(item) },
                )
            }

            // TODO Add condition for displaying logout button if user identity / auth present
            MenuElement(
                text = stringResource(Res.string.action_logout),
                imageVector = Icons.AutoMirrored.Rounded.Logout,
                modifier = Modifier.fillMaxWidth(),
                onClick = onLogout,
            )
        }
    }
    VerticalDivider()
}

@Composable
private fun MenuElement(
    text: String,
    imageVector: ImageVector,
    modifier: Modifier = Modifier,
    selected: Boolean = false,
    enabled: Boolean = true,
    onClick: () -> Unit = {},
) {
    val alpha = if (enabled) 1f else 0.38f
    Tab(
        modifier = modifier.background(
            if (selected) {
                MaterialTheme.colorScheme.primaryContainer.copy(alpha = alpha)
            } else {
                Color.Unspecified
            },
        ),
        selected = selected,
        enabled = enabled,
        selectedContentColor = MaterialTheme.colorScheme.onPrimaryContainer.copy(alpha = alpha),
        unselectedContentColor = MaterialTheme.colorScheme.onSurface.copy(alpha = alpha),
        onClick = onClick,
    ) {
        Row(
            modifier = Modifier.fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 12.dp),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Icon(
                imageVector = imageVector,
                contentDescription = null,
            )
            Text(
                style = MaterialTheme.typography.labelLarge,
                text = text,
            )
        }
    }
}

private fun getMainMenuText(item: MainMenu) = when (item) {
    MainMenu.Dashboard -> Res.string.nav_dashboard
    MainMenu.Metrics -> Res.string.nav_metrics
    MainMenu.Cluster -> Res.string.nav_cluster
    MainMenu.Security -> Res.string.nav_security
    MainMenu.Configsets -> Res.string.nav_configsets
    MainMenu.Collections -> Res.string.nav_collections
    MainMenu.QueriesAndOperations -> Res.string.nav_queries_and_operations
    MainMenu.Environment -> Res.string.nav_environment
    MainMenu.Logging -> Res.string.nav_logging
    MainMenu.ThreadDump -> Res.string.nav_thread_dump
}

private fun getMenuIcon(item: MainMenu) = when (item) {
    MainMenu.Dashboard -> Icons.Rounded.Dashboard
    MainMenu.Metrics -> Icons.Rounded.Analytics
    MainMenu.Cluster -> Icons.Rounded.Hub
    MainMenu.Security -> Icons.Rounded.Security
    MainMenu.Configsets -> Icons.Rounded.Folder // TODO Update to FolderData
    MainMenu.Collections -> Icons.Rounded.Apps
    MainMenu.QueriesAndOperations -> Icons.Rounded.DocumentScanner // TODO Update to OtherAdmission
    MainMenu.Environment -> Icons.Rounded.Memory
    MainMenu.Logging -> Icons.AutoMirrored.Rounded.TextSnippet
    MainMenu.ThreadDump -> Icons.AutoMirrored.Rounded.ViewList
}

/**
 * Temporary function for disabling sections in the navigation that have not been implemented yet.
 *
 * @param item The menu item to check for availability.
 * @return Returns `true` iff the user can navigate to the section.
 * TODO Remove once all sections are added
 */
private fun isSectionAvailable(item: MainMenu): Boolean = when (item) {
    MainMenu.Environment -> true
    MainMenu.Logging -> true
    else -> false
}
