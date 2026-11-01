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

package org.apache.solr.ui.views.components

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.widthIn
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
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp

/** An option shown in a [HeaderDropdown]. */
data class HeaderDropdownOption(
    val key: String,
    val label: String,
    val subtitle: String? = null,
)

/** The available options, loading state, selection, and selection callback for a [HeaderDropdown]. */
data class HeaderDropdownState(
    val options: List<HeaderDropdownOption>,
    val isLoading: Boolean,
    val selectedKey: String?,
    val onSelect: (String) -> Unit,
    val isEnabled: Boolean = true,
)

/**
 * A reusable dropdown field for compact headers, with an optional subtitle for each option.
 *
 * @param label Label shown on the field.
 * @param state Options, loading state, selection, and callback for the dropdown.
 * @param testTag Test tag applied to the field.
 * @param modifier Modifier applied to the dropdown container.
 * @param placeholder Optional text shown when there is no selection.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun HeaderDropdown(
    label: String,
    state: HeaderDropdownState,
    testTag: String,
    modifier: Modifier = Modifier,
    placeholder: String = "",
) {
    var expanded by remember { mutableStateOf(false) }
    val enabled = state.isEnabled && !state.isLoading && state.options.isNotEmpty()
    val selectedOption = state.options.firstOrNull { it.key == state.selectedKey }

    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = { expanded = it },
        modifier = modifier,
    ) {
        OutlinedTextField(
            value = selectedOption?.label ?: state.selectedKey.orEmpty(),
            onValueChange = {},
            readOnly = true,
            enabled = enabled,
            singleLine = true,
            label = { Text(label) },
            placeholder = if (state.selectedKey.isNullOrEmpty() && placeholder.isNotEmpty()) {
                { Text(placeholder) }
            } else {
                null
            },
            trailingIcon = {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    selectedOption?.subtitle?.let {
                        Text(
                            text = it,
                            style = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.widthIn(max = 160.dp),
                        )
                    }
                    ExposedDropdownMenuDefaults.TrailingIcon(expanded)
                }
            },
            modifier = Modifier
                .fillMaxWidth()
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
            state.options.forEach { option ->
                DropdownMenuItem(
                    modifier = Modifier.testTag("${testTag}_item_${option.key}"),
                    text = {
                        Column {
                            Text(text = option.label, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            option.subtitle?.let {
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
                        state.onSelect(option.key)
                        expanded = false
                    },
                )
            }
        }
    }
}
