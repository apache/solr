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
package org.apache.solr.ui.views.collections

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.KeyboardOptions
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
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.action_cancel
import org.apache.solr.ui.shared.generated.resources.action_create_collection
import org.apache.solr.ui.shared.generated.resources.label_collection_name
import org.apache.solr.ui.shared.generated.resources.label_replication_factor
import org.apache.solr.ui.shared.generated.resources.label_shards
import org.apache.solr.ui.shared.generated.resources.title_create_collection
import org.apache.solr.ui.views.components.SolrButton
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrTextButton
import org.apache.solr.ui.views.configsets.ConfigsetsDropdown
import org.jetbrains.compose.resources.stringResource

/**
 * Form to create a collection from an existing configset. The form keeps its typed values; the
 * caller supplies the configsets, the error messages, and the actions.
 *
 * @param configsets Configsets to choose from. The first one is selected by default.
 * @param nameError Message shown under the name field, for example when the name exists.
 * @param generalError Message shown when creation fails for another reason.
 * @param onCreate Called with the name, the configset, the shard count and the replication factor.
 * @param onCancel Called when the user cancels.
 * @param modifier Modifier to apply to the root component.
 */
@Composable
fun CreateCollectionForm(
    configsets: List<Configset>,
    nameError: String?,
    generalError: String?,
    onCreate: (name: String, configset: String, shards: Int, replicationFactor: Int) -> Unit,
    onCancel: () -> Unit,
    modifier: Modifier = Modifier,
) {
    var name by remember { mutableStateOf("") }
    var configset by remember { mutableStateOf(configsets.firstOrNull()?.name) }
    var shards by remember { mutableStateOf("1") }
    var replicationFactor by remember { mutableStateOf("1") }
    val shardCount = shards.toIntOrNull()?.takeIf { it > 0 }
    val replicas = replicationFactor.toIntOrNull()?.takeIf { it > 0 }
    val selected = configset

    SolrCard(
        modifier = modifier,
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text(
            text = stringResource(Res.string.title_create_collection),
            style = MaterialTheme.typography.headlineSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        OutlinedTextField(
            modifier = Modifier.fillMaxWidth().testTag("create_collection_name_field"),
            value = name,
            onValueChange = { name = it },
            label = { Text(stringResource(Res.string.label_collection_name)) },
            singleLine = true,
            isError = nameError != null,
            supportingText = nameError?.let { { Text(it) } },
        )
        ConfigsetsDropdown(
            modifier = Modifier.fillMaxWidth(),
            selectedConfigset = selected,
            configsets = configsets,
            selectConfigset = { configset = it },
        )
        Row(
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_shards_field"),
                value = shards,
                onValueChange = { shards = it },
                label = { Text(stringResource(Res.string.label_shards)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_replicas_field"),
                value = replicationFactor,
                onValueChange = { replicationFactor = it },
                label = { Text(stringResource(Res.string.label_replication_factor)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
        }
        generalError?.let {
            Text(
                text = it,
                color = MaterialTheme.colorScheme.error,
            )
        }
        Row(
            modifier = Modifier.padding(top = 16.dp).fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
        ) {
            SolrTextButton(onClick = onCancel) {
                Text(stringResource(Res.string.action_cancel))
            }
            SolrButton(
                modifier = Modifier.testTag("create_collection_button"),
                onClick = {
                    if (selected != null && shardCount != null && replicas != null) {
                        onCreate(name, selected, shardCount, replicas)
                    }
                },
                enabled = name.isNotBlank() && selected != null && shardCount != null && replicas != null,
            ) {
                Text(stringResource(Res.string.action_create_collection))
            }
        }
    }
}
