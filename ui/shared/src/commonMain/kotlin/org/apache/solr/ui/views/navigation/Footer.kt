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
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.broken_image
import org.apache.solr.ui.generated.resources.bug_report
import org.apache.solr.ui.generated.resources.code
import org.apache.solr.ui.generated.resources.community
import org.apache.solr.ui.generated.resources.documentation
import org.apache.solr.ui.generated.resources.groups
import org.apache.solr.ui.generated.resources.info
import org.apache.solr.ui.generated.resources.issue_tracker
import org.apache.solr.ui.generated.resources.menu_book
import org.apache.solr.ui.generated.resources.slack
import org.apache.solr.ui.generated.resources.solr_query_syntax
import org.apache.solr.ui.generated.resources.support
import org.jetbrains.compose.resources.DrawableResource
import org.jetbrains.compose.resources.StringResource
import org.jetbrains.compose.resources.painterResource
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

        val elements = listOf(
            FooterElement(
                drawable = Res.drawable.menu_book,
                stringRes = Res.string.documentation,
                uri = "https://solr.apache.org/guide/solr/latest/index.html",
            ),
            FooterElement(
                drawable = Res.drawable.code,
                stringRes = Res.string.solr_query_syntax,
                uri = "https://solr.apache.org/guide/solr/latest/query-guide/query-syntax-and-parsers.html",
            ),
            FooterElement(
                drawable = Res.drawable.bug_report,
                stringRes = Res.string.issue_tracker,
                uri = "https://issues.apache.org/jira/projects/SOLR",
            ),
            FooterElement(
                drawable = Res.drawable.groups,
                stringRes = Res.string.community,
                uri = "https://solr.apache.org/community.html",
            ),
            FooterElement(
                drawable = Res.drawable.broken_image, // TODO Add Slack Logo
                stringRes = Res.string.slack,
                uri = "https://the-asf.slack.com/messages/CEKUCUNE9",
            ),
            FooterElement(
                drawable = Res.drawable.info,
                stringRes = Res.string.support,
                uri = "https://solr.apache.org/community.html#support",
            ),
        )

        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.Center,
        ) {
            val uriHandler = LocalUriHandler.current
            elements.forEach { element ->
                FooterAction(
                    drawable = element.drawable,
                    stringRes = element.stringRes,
                    iconOnly = showIconsOnly,
                    onClick = { uriHandler.openUri(element.uri) },
                )
            }
        }
    }
}

@Composable
private fun FooterAction(
    drawable: DrawableResource,
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
            painter = painterResource(drawable),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        if (!iconOnly) Text(stringResource(stringRes))
    }
}

private data class FooterElement(
    val drawable: DrawableResource,
    val stringRes: StringResource,
    val uri: String,
)
