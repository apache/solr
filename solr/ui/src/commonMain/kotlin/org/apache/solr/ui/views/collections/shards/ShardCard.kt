package org.apache.solr.ui.views.collections.shards

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Add
import androidx.compose.material.icons.outlined.ExpandMore
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedCard
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.rotate
import androidx.compose.ui.unit.dp
import kotlin.collections.component1
import kotlin.collections.component2
import org.apache.solr.ui.components.collections.data.Shard
import org.apache.solr.ui.views.collections.shards.replica.AddReplicaDialog
import org.apache.solr.ui.views.collections.shards.replica.ReplicaListItem

@Composable
fun ShardCard(
    shardName: String,
    shard: Shard,
    expanded: Boolean,
    onToggle: () -> Unit,
    modifier: Modifier = Modifier,
    liveNodes: List<String> = emptyList(),
    onFetchLiveNodes: () -> Unit = {},
    onAddReplica: (shardName: String, node: String?, type: String) -> Unit = { _, _, _ -> },
    onDeleteReplica: (shardName: String, replica: String) -> Unit = { _, _ -> },
) {
    OutlinedCard(
        shape = MaterialTheme.shapes.medium,
        colors = CardDefaults.outlinedCardColors(),
    ) {
        // Header
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .clickable { onToggle() }
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            val rotation by animateFloatAsState(if (expanded) 180f else 0f, label = "expand-rot")
            Icon(
                Icons.Outlined.ExpandMore,
                contentDescription = null,
                modifier = Modifier.rotate(rotation),
            )
            Spacer(Modifier.width(8.dp))
            HealthDot(status = shard.health ?: shard.state)
            Spacer(Modifier.width(8.dp))
            Text("Shard: $shardName", style = MaterialTheme.typography.titleSmall)
            Spacer(Modifier.width(8.dp))
            val meta = listOfNotNull(
                shard.state?.let { "state=$it" },
                shard.range?.let { "range=$it" },
            ).joinToString("   ")
            if (meta.isNotBlank()) {
                Text(
                    meta,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }

            var showAdd by remember { mutableStateOf(false) }
            var addForShard by remember { mutableStateOf<String?>(null) }

            Spacer(Modifier.weight(1f))
            TextButton(onClick = {
                addForShard = shardName
                showAdd = true
            }) {
                // ← the button
                Icon(Icons.Outlined.Add, contentDescription = null)
                Spacer(Modifier.width(6.dp))
                Text("Add replica")
            }

            AddReplicaDialog(
                visible = showAdd,
                shardName = addForShard ?: "",
                liveNodes = liveNodes,
                onFetchLiveNodes = onFetchLiveNodes,
                onDismiss = { showAdd = false },
                onConfirm = { node, type ->
                    onAddReplica(addForShard!!, node, type)
                    showAdd = false
                },
            )
        }

        // Body (replicas)
        AnimatedVisibility(visible = expanded) {
            Column(Modifier.fillMaxWidth().padding(bottom = 8.dp)) {
                val replicaEntries = shard.replicas.entries.sortedBy { entry ->
                    entry.key.replace("shard", "").toIntOrNull() ?: Int.MAX_VALUE
                }
                if (replicaEntries.isEmpty()) {
                    HorizontalDivider()
                    Text(
                        "No replicas",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp),
                    )
                } else {
                    HorizontalDivider()
                    replicaEntries.forEachIndexed { idx, (replicaName, rep) ->
                        ReplicaListItem(
                            replicaName = replicaName,
                            rep = rep,
                            onDelete = { onDeleteReplica(shardName, replicaName) },
                        )
                        if (idx != replicaEntries.lastIndex) HorizontalDivider()
                    }
                }
            }
        }
    }
}
