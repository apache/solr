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
 */package org.apache.solr.ui.views.files

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.chevron_right
import org.apache.solr.ui.shared.generated.resources.expand_more
import org.apache.solr.ui.shared.generated.resources.folder_data
import org.jetbrains.compose.resources.painterResource

sealed interface FileTreeNode {
    val name: String
    val path: String
}

data class FileTreeFile(
    override val name: String,
    override val path: String,
) : FileTreeNode

data class FileTreeFolder(
    override val name: String,
    override val path: String,
    val children: List<FileTreeNode>,
) : FileTreeNode

@Composable
fun FileTree(
    nodes: List<FileTreeNode>,
    selectedPath: String?,
    onItemClick: (FileTreeNode) -> Unit,
    onItemDoubleClick: (FileTreeNode) -> Unit,
    modifier: Modifier = Modifier,
) {
    var expandedPaths by remember { mutableStateOf(emptySet<String>()) }
    val rows = visibleRows(nodes, expandedPaths)
    val toggle: (String) -> Unit = { path ->
        expandedPaths = if (path in expandedPaths) expandedPaths - path else expandedPaths + path
    }
    LazyColumn(modifier = modifier) {
        items(rows, key = { it.node.path }) { row ->
            FileTreeRow(
                row = row,
                selected = row.node.path == selectedPath,
                onToggle = { toggle(row.node.path) },
                onClick = { onItemClick(row.node) },
                onDoubleClick = {
                    if (row.node is FileTreeFolder) toggle(row.node.path)
                    onItemDoubleClick(row.node)
                },
            )
        }
    }
}

private class VisibleRow(
    val node: FileTreeNode,
    val depth: Int,
    val expanded: Boolean,
)

private fun visibleRows(
    nodes: List<FileTreeNode>,
    expandedPaths: Set<String>,
    depth: Int = 0,
): List<VisibleRow> = nodes.flatMap { node ->
    val folder = node as? FileTreeFolder
    val expanded = folder != null && node.path in expandedPaths
    val row = VisibleRow(node, depth, expanded)
    if (folder != null && expanded) {
        listOf(row) + visibleRows(folder.children, expandedPaths, depth + 1)
    } else {
        listOf(row)
    }
}

@Composable
private fun FileTreeRow(
    row: VisibleRow,
    selected: Boolean,
    onToggle: () -> Unit,
    onClick: () -> Unit,
    onDoubleClick: () -> Unit,
) {
    val node = row.node
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(if (selected) MaterialTheme.colorScheme.secondaryContainer else Color.Transparent)
            .combinedClickable(onClick = onClick, onDoubleClick = onDoubleClick)
            .padding(start = 8.dp + 16.dp * row.depth, end = 8.dp, top = 4.dp, bottom = 4.dp)
            .testTag("item_${node.path}"),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        if (node is FileTreeFolder) {
            Icon(
                painter = painterResource(if (row.expanded) Res.drawable.expand_more else Res.drawable.chevron_right),
                contentDescription = null,
                modifier = Modifier
                    .size(16.dp)
                    .clickable(onClick = onToggle)
                    .testTag("chevron_${node.path}"),
            )
            Spacer(Modifier.width(4.dp))
            Icon(
                painter = painterResource(Res.drawable.folder_data),
                contentDescription = null,
                modifier = Modifier.size(16.dp),
            )
        } else {
            Spacer(Modifier.width(20.dp))
            FileTypeIcon(
                fileType = node.name.substringAfterLast('.', ""),
                modifier = Modifier.size(16.dp),
            )
        }
        Spacer(Modifier.width(8.dp))
        Text(
            text = node.name,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurface,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
    }
}
