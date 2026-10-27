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
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
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
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.action_cancel
import org.apache.solr.ui.shared.generated.resources.action_create_collection
import org.apache.solr.ui.shared.generated.resources.cd_decrease_shards
import org.apache.solr.ui.shared.generated.resources.cd_increase_shards
import org.apache.solr.ui.shared.generated.resources.helper_collection_name
import org.apache.solr.ui.shared.generated.resources.label_advanced_options
import org.apache.solr.ui.shared.generated.resources.label_copies_of_each_shard
import org.apache.solr.ui.shared.generated.resources.label_name
import org.apache.solr.ui.shared.generated.resources.label_nrt_replicas
import org.apache.solr.ui.shared.generated.resources.label_placement
import org.apache.solr.ui.shared.generated.resources.label_pull_replicas
import org.apache.solr.ui.shared.generated.resources.label_put_replicas_on
import org.apache.solr.ui.shared.generated.resources.label_route_field
import org.apache.solr.ui.shared.generated.resources.label_router
import org.apache.solr.ui.shared.generated.resources.label_shards
import org.apache.solr.ui.shared.generated.resources.label_size_and_redundancy
import org.apache.solr.ui.shared.generated.resources.label_tlog_replicas
import org.apache.solr.ui.shared.generated.resources.option_placement_any_node_spread_zones
import org.apache.solr.ui.shared.generated.resources.option_router_composite_id
import org.apache.solr.ui.shared.generated.resources.option_router_implicit
import org.apache.solr.ui.shared.generated.resources.title_create_collection
import org.apache.solr.ui.views.components.SolrButton
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrTextButton
import org.apache.solr.ui.views.configsets.ConfigsetsDropdown
import org.jetbrains.compose.resources.stringResource

data class CreateCollectionInput(
    val name: String,
    val configset: String,
    val shards: Int,
    val nrtReplicas: Int,
    val tlogReplicas: Int,
    val pullReplicas: Int,
    val router: String,
    val routeField: String?,
)

private const val ROUTER_COMPOSITE_ID = "compositeId"
private const val ROUTER_IMPLICIT = "implicit"

private val collectionNamePattern = Regex("^(?!-)[._A-Za-z0-9-]+$")

private fun String.countAtLeast(min: Int): Int? = toIntOrNull()?.takeIf { it >= min }

/**
 * Form to create a collection from an existing configset. The form keeps its typed values; the
 * caller supplies the configsets, the error messages, and the actions.
 *
 * @param configsets Configsets to choose from. The first one is selected by default.
 * @param nameError Message shown under the name field, for example when the name exists.
 * @param generalError Message shown when creation fails for another reason.
 * @param onCreate Called with the values to create the collection with.
 * @param onCancel Called when the user cancels.
 * @param modifier Modifier to apply to the root component.
 */
@Composable
fun CreateCollectionForm(
    configsets: List<Configset>,
    nameError: String?,
    generalError: String?,
    onCreate: (CreateCollectionInput) -> Unit,
    onCancel: () -> Unit,
    modifier: Modifier = Modifier,
) {
    var name by remember { mutableStateOf("") }
    var configset by remember { mutableStateOf(configsets.firstOrNull()?.name) }
    var shards by remember { mutableStateOf("2") }
    var nrt by remember { mutableStateOf("2") }
    var tlog by remember { mutableStateOf("0") }
    var pull by remember { mutableStateOf("1") }
    var advancedOpen by remember { mutableStateOf(false) }
    var router by remember { mutableStateOf(ROUTER_COMPOSITE_ID) }
    var routeField by remember { mutableStateOf("") }

    val nameValid = collectionNamePattern.matches(name)
    val shardCount = shards.countAtLeast(1)
    val nrtCount = nrt.countAtLeast(0)
    val tlogCount = tlog.countAtLeast(0)
    val pullCount = pull.countAtLeast(0)
    val selected = configset
    val decreaseShards = stringResource(Res.string.cd_decrease_shards)
    val increaseShards = stringResource(Res.string.cd_increase_shards)
    val input =
        if (
            nameValid &&
                selected != null &&
                shardCount != null &&
                nrtCount != null &&
                tlogCount != null &&
                pullCount != null &&
                nrtCount + tlogCount >= 1
        ) {
            CreateCollectionInput(
                name = name,
                configset = selected,
                shards = shardCount,
                nrtReplicas = nrtCount,
                tlogReplicas = tlogCount,
                pullReplicas = pullCount,
                router = router,
                routeField = routeField.ifBlank { null },
            )
        } else {
            null
        }

    SolrCard(
        modifier = modifier.verticalScroll(rememberScrollState()),
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
            label = { Text(stringResource(Res.string.label_name)) },
            singleLine = true,
            isError = nameError != null || (name.isNotEmpty() && !nameValid),
            supportingText = {
                Text(nameError ?: stringResource(Res.string.helper_collection_name))
            },
        )
        ConfigsetsDropdown(
            modifier = Modifier.fillMaxWidth(),
            selectedConfigset = selected,
            configsets = configsets,
            selectConfigset = { configset = it },
        )

        Text(
            text = stringResource(Res.string.label_size_and_redundancy),
            style = MaterialTheme.typography.titleSmall,
        )
        Row(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            SolrTextButton(
                onClick = { shards = ((shards.toIntOrNull() ?: 1) - 1).coerceAtLeast(1).toString() },
                enabled = (shards.toIntOrNull() ?: 0) > 1,
                modifier = Modifier.testTag("create_collection_shards_decrease").semantics {
                    contentDescription = decreaseShards
                },
            ) {
                Text("-")
            }
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_shards_field"),
                value = shards,
                onValueChange = { shards = it },
                label = { Text(stringResource(Res.string.label_shards)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
            SolrTextButton(
                onClick = { shards = ((shards.toIntOrNull() ?: 0) + 1).toString() },
                modifier = Modifier.testTag("create_collection_shards_increase").semantics {
                    contentDescription = increaseShards
                },
            ) {
                Text("+")
            }
        }

        Text(
            text = stringResource(Res.string.label_copies_of_each_shard),
            style = MaterialTheme.typography.titleSmall,
        )
        Row(
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_nrt_field"),
                value = nrt,
                onValueChange = { nrt = it },
                label = { Text(stringResource(Res.string.label_nrt_replicas)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_tlog_field"),
                value = tlog,
                onValueChange = { tlog = it },
                label = { Text(stringResource(Res.string.label_tlog_replicas)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
            OutlinedTextField(
                modifier = Modifier.weight(1f).testTag("create_collection_pull_field"),
                value = pull,
                onValueChange = { pull = it },
                label = { Text(stringResource(Res.string.label_pull_replicas)) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            )
        }

        Text(
            text = stringResource(Res.string.label_placement),
            style = MaterialTheme.typography.titleSmall,
        )
        OutlinedTextField(
            modifier = Modifier.fillMaxWidth().testTag("create_collection_placement_field"),
            value = stringResource(Res.string.option_placement_any_node_spread_zones),
            onValueChange = {},
            readOnly = true,
            label = { Text(stringResource(Res.string.label_put_replicas_on)) },
            singleLine = true,
        )

        SolrTextButton(
            onClick = { advancedOpen = !advancedOpen },
            modifier = Modifier.testTag("create_collection_advanced_toggle"),
        ) {
            Text(stringResource(Res.string.label_advanced_options))
        }
        if (advancedOpen) {
            Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
                RouterDropdown(
                    router = router,
                    selectRouter = { router = it },
                )
                OutlinedTextField(
                    modifier = Modifier.fillMaxWidth().testTag("create_collection_route_field"),
                    value = routeField,
                    onValueChange = { routeField = it },
                    label = { Text(stringResource(Res.string.label_route_field)) },
                    singleLine = true,
                )
            }
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
                onClick = { input?.let(onCreate) },
                enabled = input != null,
            ) {
                Text(stringResource(Res.string.action_create_collection))
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun RouterDropdown(
    router: String,
    selectRouter: (String) -> Unit,
    modifier: Modifier = Modifier,
) {
    var expanded by remember { mutableStateOf(false) }
    val options =
        listOf(
            ROUTER_COMPOSITE_ID to stringResource(Res.string.option_router_composite_id),
            ROUTER_IMPLICIT to stringResource(Res.string.option_router_implicit),
        )

    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = { expanded = it },
        modifier = modifier.fillMaxWidth().testTag("create_collection_router_dropdown"),
    ) {
        OutlinedTextField(
            modifier = Modifier
                .menuAnchor(
                    type = ExposedDropdownMenuAnchorType.PrimaryNotEditable,
                    enabled = true,
                )
                .fillMaxWidth(),
            value = options.first { it.first == router }.second,
            onValueChange = {},
            readOnly = true,
            label = { Text(stringResource(Res.string.label_router)) },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            singleLine = true,
        )
        ExposedDropdownMenu(
            expanded = expanded,
            onDismissRequest = { expanded = false },
        ) {
            options.forEach { (value, label) ->
                DropdownMenuItem(
                    text = { Text(label) },
                    onClick = {
                        selectRouter(value)
                        expanded = false
                    },
                )
            }
        }
    }
}
