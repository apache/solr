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
 */package org.apache.solr.ui.views.collections

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.CollectionDetails
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.collection_details
import org.apache.solr.ui.shared.generated.resources.label_auto_add_replicas
import org.apache.solr.ui.shared.generated.resources.label_collection_name
import org.apache.solr.ui.shared.generated.resources.label_config_name
import org.apache.solr.ui.shared.generated.resources.label_max_shards_per_node
import org.apache.solr.ui.shared.generated.resources.label_replication_factor
import org.apache.solr.ui.shared.generated.resources.label_router
import org.apache.solr.ui.shared.generated.resources.label_shard_count
import org.apache.solr.ui.shared.generated.resources.state_disabled
import org.apache.solr.ui.shared.generated.resources.state_enabled
import org.apache.solr.ui.views.components.SolrCard
import org.jetbrains.compose.resources.stringResource

@Composable
fun CollectionDetailsCard(
    details: CollectionDetails,
    modifier: Modifier = Modifier,
) {
    SolrCard(modifier = modifier.fillMaxWidth()) {
        Text(
            text = stringResource(Res.string.collection_details),
            style = MaterialTheme.typography.titleMedium,
            color = MaterialTheme.colorScheme.onSurface,
        )
        Spacer(Modifier.height(12.dp))
        SelectionContainer {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                DetailRow(stringResource(Res.string.label_collection_name), details.name)
                DetailRow(stringResource(Res.string.label_config_name), details.configName)
                DetailRow(stringResource(Res.string.label_shard_count), details.shardCount.toString())
                DetailRow(stringResource(Res.string.label_replication_factor), details.replicationFactor.toString())
                DetailRow(stringResource(Res.string.label_router), details.routerName)
                DetailRow(stringResource(Res.string.label_max_shards_per_node), details.maxShardsPerNode.toString())
                DetailRow(
                    stringResource(Res.string.label_auto_add_replicas),
                    stringResource(if (details.autoAddReplicas) Res.string.state_enabled else Res.string.state_disabled),
                )
            }
        }
    }
}

@Composable
private fun DetailRow(label: String, value: String) {
    Row(modifier = Modifier.fillMaxWidth()) {
        Text(
            text = label,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.width(200.dp),
        )
        Text(
            text = value,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurface,
        )
    }
}
