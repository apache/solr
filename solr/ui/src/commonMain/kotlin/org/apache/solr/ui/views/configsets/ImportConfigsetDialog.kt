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

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_cancel
import org.apache.solr.ui.generated.resources.action_create_configset
import org.apache.solr.ui.generated.resources.action_import_configset
import org.apache.solr.ui.generated.resources.label_configset_name
import org.apache.solr.ui.generated.resources.label_select_configset_file
import org.apache.solr.ui.generated.resources.title_import_configset
import org.apache.solr.ui.views.components.SolrButton
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrTextButton
import org.apache.solr.ui.views.files.FileSelector
import org.jetbrains.compose.resources.stringResource

@Composable
fun ImportConfigsetDialog(
    component: ImportConfigsetComponent,
    onDismissRequest: () -> Unit,
    onCreate: () -> Unit,
    modifier: Modifier = Modifier,
) = Dialog(onDismissRequest = onDismissRequest) {
    val model by component.model.collectAsState()
    val filePickerModel by component.filePicker.model.collectAsState()

    SolrCard(
        modifier = modifier,
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text(
            text = stringResource(Res.string.title_import_configset),
            style = MaterialTheme.typography.headlineSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            FileSelector(
                modifier = Modifier.weight(1f),
                component = component.filePicker,
                label = stringResource(Res.string.label_select_configset_file),
                selectFileText = stringResource(Res.string.label_select_configset_file),
            )
            val configsetName = model.configsetName.ifBlank {
                filePickerModel.selectedFile?.name ?: ""
            }
            OutlinedTextField(
                modifier = Modifier.weight(1f),
                value = configsetName,
                onValueChange = component::onConfigsetNameChange,
                label = { Text(stringResource(Res.string.label_configset_name)) },
                singleLine = true,
            )
        }

        Row(
            modifier = Modifier.padding(top = 16.dp).fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
        ) {
            // Dialog actions
            SolrTextButton(onClick = onCreate) {
                Text(stringResource(Res.string.action_create_configset))
            }

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                SolrTextButton(onClick = onDismissRequest) {
                    Text(stringResource(Res.string.action_cancel))
                }
                SolrButton(
                    onClick = component::onImportConfigset,
                    enabled = filePickerModel.selectedFile != null && !model.isLoading,
                ) {
                    Text(stringResource(Res.string.action_import_configset))
                }
            }
        }
    }
}
