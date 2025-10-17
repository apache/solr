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

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.MenuBook
import androidx.compose.material.icons.rounded.BugReport
import androidx.compose.material.icons.rounded.Code
import androidx.compose.material.icons.rounded.Dashboard
import androidx.compose.material.icons.rounded.Groups
import androidx.compose.material.icons.rounded.ImageNotSupported
import androidx.compose.material.icons.rounded.Support
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.platform.UriHandler
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.community
import org.apache.solr.ui.generated.resources.documentation
import org.apache.solr.ui.generated.resources.issue_tracker
import org.apache.solr.ui.generated.resources.slack
import org.apache.solr.ui.generated.resources.solr_query_syntax
import org.apache.solr.ui.generated.resources.support
import org.jetbrains.compose.resources.StringResource
import org.jetbrains.compose.resources.stringResource

/**
 * The basic footer shown in all pages.
 *
 * @param modifier Modifier to apply to the root composable.
 * @param collapseWidth The width at which the footer should be collapsed and display only icons.
 */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun Footer(
    modifier: Modifier = Modifier,
    collapseWidth: Dp = 1024.dp,
) = Column(modifier = modifier) {
    HorizontalDivider()

    BoxWithConstraints {
        val showIconsOnly = maxWidth < collapseWidth

        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.Center,
        ) {
            val uriHandler = LocalUriHandler.current
            FooterAction(
                imageVector = Icons.AutoMirrored.Rounded.MenuBook,
                stringRes = Res.string.documentation,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://solr.apache.org/guide/solr/latest/index.html") },
            )

            FooterAction(
                imageVector = Icons.Rounded.Code,
                stringRes = Res.string.solr_query_syntax,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://solr.apache.org/guide/solr/latest/query-guide/query-syntax-and-parsers.html") },
            )

            FooterAction(
                imageVector = Icons.Rounded.BugReport,
                stringRes = Res.string.issue_tracker,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://issues.apache.org/jira/projects/SOLR") },
            )

            FooterAction(
                imageVector = Icons.Rounded.Groups,
                stringRes = Res.string.community,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://solr.apache.org/community.html") },
            )

            FooterAction(
                imageVector = Icons.Rounded.ImageNotSupported, // TODO Add Slack Logo
                stringRes = Res.string.slack,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://the-asf.slack.com/messages/CEKUCUNE9") },
            )

            FooterAction(
                imageVector = Icons.Rounded.Support,
                stringRes = Res.string.support,
                iconOnly = showIconsOnly,
                onClick = { uriHandler.openUri("https://solr.apache.org/community.html#support") },
            )
        }
    }
}

@Composable
private fun FooterAction(
    imageVector: ImageVector,
    stringRes: StringResource,
    iconOnly: Boolean = false,
    onClick: () -> Unit = {},
) = CompositionLocalProvider(LocalContentColor provides MaterialTheme.colorScheme.onSurface) {
    Row(
        modifier = Modifier.clickable(onClick = onClick)
            .padding(horizontal = 16.dp, vertical = 12.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Icon(
            imageVector = imageVector,
            contentDescription = null,
        )
        if (!iconOnly) Text(stringResource(stringRes))
    }
}
