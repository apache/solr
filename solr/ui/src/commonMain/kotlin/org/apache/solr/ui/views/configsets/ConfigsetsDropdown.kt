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
package org.apache.solr.ui.views.configsets

import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.MenuAnchorType
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
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.nav_configsets
import org.apache.solr.ui.generated.resources.no_configsets
import org.jetbrains.compose.resources.stringResource

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ConfigsetsDropdown(
    selectedConfigSet: String,
    selectConfigset: (String) -> Unit,
    availableConfigsets: List<Configset>,
    modifier: Modifier = Modifier,
) {
    var expanded by remember { mutableStateOf(false) }
    val enabled = availableConfigsets.isNotEmpty()

    Row(
        modifier = modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        ExposedDropdownMenuBox(
            expanded = expanded,
            onExpandedChange = { expanded = it },
            modifier = Modifier.widthIn(min = 256.dp).weight(1f),
        ) {
            OutlinedTextField(
                value = selectedConfigSet,
                onValueChange = {},
                readOnly = true,
                enabled = enabled,
                label = { Text(stringResource(Res.string.nav_configsets)) },
                placeholder = {
                    if (availableConfigsets.isEmpty()) {
                        Text(
                            modifier = Modifier.testTag("no_configsets_placeholder"),
                            text = stringResource(Res.string.no_configsets),
                        )
                    }
                },
                trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded) },
                modifier = Modifier
                    .menuAnchor(
                        type = MenuAnchorType.PrimaryNotEditable,
                        enabled = enabled,
                    )
                    .fillMaxWidth()
                    .testTag("configsets_dropdown"),
            )
            ExposedDropdownMenu(
                modifier = Modifier.testTag("configsets_exposed_dropdown_menu"),
                expanded = expanded,
                onDismissRequest = { expanded = false },
            ) {
                availableConfigsets.forEach { configset ->
                    DropdownMenuItem(
                        modifier = Modifier.testTag(tag = configset.name),
                        text = { Text(configset.name) },
                        onClick = {
                            selectConfigset(configset.name)
                            expanded = false
                        },
                    )
                }
            }
        }
    }
}
