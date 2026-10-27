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
 */package org.apache.solr.ui.views.indexquery

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuAnchorType
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.RequestHandlerOption
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.label_request_handler
import org.apache.solr.ui.shared.generated.resources.nav_collections
import org.apache.solr.ui.shared.generated.resources.no_collections_available
import org.apache.solr.ui.shared.generated.resources.no_request_handlers
import org.apache.solr.ui.shared.generated.resources.placeholder_select_request_handler
import org.jetbrains.compose.resources.stringResource

/**
 * Header of the Index & Query section with a collection dropdown and a request handler dropdown.
 *
 * The header only renders what it is given. Loading and selection happen in the caller.
 *
 * @param collections The available collection names.
 * @param collectionsLoading Whether the collections are being loaded. Disables both dropdowns.
 * @param selectedCollection The selected collection, or null.
 * @param onSelectCollection Called with the collection the user picked.
 * @param handlers The request handlers of the selected collection.
 * @param handlersLoading Whether the handlers are being loaded. Disables the handler dropdown.
 * @param selectedHandler The name of the selected request handler, or null.
 * @param onSelectHandler Called with the name of the request handler the user picked.
 * @param modifier Modifier to apply to the root column.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun IndexQueryHeader(
    collections: List<String>,
    collectionsLoading: Boolean,
    selectedCollection: String?,
    onSelectCollection: (String) -> Unit,
    handlers: List<RequestHandlerOption>,
    handlersLoading: Boolean,
    selectedHandler: String?,
    onSelectHandler: (String) -> Unit,
    modifier: Modifier = Modifier,
) {
    val collectionsEnabled = !collectionsLoading && collections.isNotEmpty()
    val handlersEnabled = !collectionsLoading &&
        selectedCollection != null &&
        !handlersLoading &&
        handlers.isNotEmpty()

    Column(modifier = modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Row(horizontalArrangement = Arrangement.spacedBy(16.dp)) {
            HeaderDropdown(
                label = stringResource(Res.string.nav_collections),
                value = selectedCollection,
                placeholder = "",
                enabled = collectionsEnabled,
                entries = collections.map { DropdownEntry(key = it, title = it) },
                onSelect = onSelectCollection,
                testTag = "index_query_collection_dropdown",
                modifier = Modifier,
            )
            HeaderDropdown(
                label = stringResource(Res.string.label_request_handler),
                value = selectedHandler,
                placeholder = stringResource(Res.string.placeholder_select_request_handler),
                enabled = handlersEnabled,
                entries = handlers.map {
                    DropdownEntry(key = it.name, title = it.name, subtitle = it.className)
                },
                onSelect = onSelectHandler,
                testTag = "index_query_handler_dropdown",
                modifier = Modifier,
            )
        }
        if (!collectionsLoading && collections.isEmpty()) {
            Text(
                text = stringResource(Res.string.no_collections_available),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag("index_query_no_collections"),
            )
        }
        if (selectedCollection != null && !handlersLoading && handlers.isEmpty()) {
            Text(
                text = stringResource(Res.string.no_request_handlers),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag("index_query_no_handlers"),
            )
        }
    }
}

private data class DropdownEntry(
    val key: String,
    val title: String,
    val subtitle: String? = null,
)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun HeaderDropdown(
    label: String,
    value: String?,
    placeholder: String,
    enabled: Boolean,
    entries: List<DropdownEntry>,
    onSelect: (String) -> Unit,
    testTag: String,
    modifier: Modifier,
) {
    var expanded by remember { mutableStateOf(false) }
    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = { expanded = it },
        modifier = modifier,
    ) {
        OutlinedTextField(
            value = value.orEmpty(),
            onValueChange = {},
            readOnly = true,
            enabled = enabled,
            singleLine = true,
            label = { Text(label) },
            placeholder = if (value.isNullOrEmpty() && placeholder.isNotEmpty()) {
                { Text(placeholder) }
            } else {
                null
            },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded) },
            modifier = Modifier
                .menuAnchor(
                    type = ExposedDropdownMenuAnchorType.PrimaryNotEditable,
                    enabled = enabled,
                )
                .testTag(testTag),
        )
        ExposedDropdownMenu(
            expanded = expanded,
            onDismissRequest = { expanded = false },
        ) {
            entries.forEach { entry ->
                DropdownMenuItem(
                    modifier = Modifier.testTag("${testTag}_item_${entry.key}"),
                    text = {
                        Column {
                            Text(text = entry.title, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            entry.subtitle?.let {
                                Text(
                                    text = it,
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    maxLines = 1,
                                    overflow = TextOverflow.Ellipsis,
                                )
                            }
                        }
                    },
                    onClick = {
                        onSelect(entry.key)
                        expanded = false
                    },
                )
            }
        }
    }
}
