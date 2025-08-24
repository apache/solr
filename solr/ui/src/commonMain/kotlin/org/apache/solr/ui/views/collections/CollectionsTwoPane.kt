package org.apache.solr.ui.views.collections

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Add
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Refresh
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.RectangleShape
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.components.collections.data.Shard

@Composable
fun CollectionsTwoPane(
    names: List<String>,
    selected: String?,
    details: CollectionData?,
    onSelect: (String) -> Unit,
    onAdd: () -> Unit,
    onDelete: () -> Unit,
    onReload: () -> Unit,
    modifier: Modifier = Modifier,
) {
    Row(Modifier.fillMaxWidth().padding(12.dp)) {
        // Left: toolbar + sidebar (fixed width)
        Column(
            Modifier.width(240.dp).border(
                BorderStroke(0.dp, Color.Transparent), // keep base
                shape = RectangleShape,
            ),
        ) {
            AddCollectionButton(onAdd = onAdd, modifier = Modifier.fillMaxWidth())
            Spacer(Modifier.height(12.dp))
            SidebarCard { CollectionsListCard(names = names, selected = selected, onSelect = onSelect) }
        }

        // between the left Column and the right panel:
        VerticalDivider(
            modifier = Modifier.fillMaxHeight(),
            color = MaterialTheme.colorScheme.outlineVariant,
            thickness = 1.dp,
        )

        Spacer(Modifier.width(16.dp))

        // Right: details panel
        Surface(tonalElevation = 0.dp, shape = MaterialTheme.shapes.medium, modifier = Modifier.weight(1f)) {
            Column(Modifier.fillMaxWidth().padding(12.dp)) {
                DetailsHeader(onDelete = onDelete, onReload = onReload)
                Spacer(Modifier.height(12.dp))
                val d = details
                if (d == null) {
                    Text("Select a collection", style = MaterialTheme.typography.titleMedium)
                    Spacer(Modifier.height(8.dp))
                    HorizontalDivider()
                } else {
                    Text("Collection: ${d.name}", style = MaterialTheme.typography.headlineSmall)
                    Spacer(Modifier.height(8.dp))
                    HorizontalDivider()
                    Spacer(Modifier.height(8.dp))
                    KeyValRow("Shard count", d.shardCount?.toString() ?: "–")
                    KeyValRow("configName", d.configName ?: "–")
                    KeyValRow("replicationFactor", d.replicationFactor?.toString() ?: "–")
                    KeyValRow("router", d.router ?: "–")
                    Spacer(Modifier.height(16.dp))
                    Text("Shards", style = MaterialTheme.typography.titleSmall)
                    HorizontalDivider()
                    Spacer(Modifier.height(8.dp))
                    ShardsSimpleCollapsible(d.shards)
                }
            }
        }
    }
}

@Composable
private fun AddCollectionButton(
    onAdd: () -> Unit,
    modifier: Modifier = Modifier,
) {
    OutlinedButton(onClick = onAdd) {
        Icon(Icons.Outlined.Add, contentDescription = null)
        Spacer(Modifier.width(8.dp))
        Text("Add Collection")
    }
}

@Composable
private fun DetailsHeader(
    onDelete: () -> Unit,
    onReload: () -> Unit,
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
    ) {
        OutlinedButton(onClick = onDelete) {
            Icon(Icons.Outlined.Delete, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Delete")
        }
        Spacer(Modifier.width(8.dp))
        OutlinedButton(onClick = onReload) {
            Icon(Icons.Outlined.Refresh, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Reload")
        }
    }
}

@Composable
private fun ActionsToolbar(onAdd: () -> Unit, onDelete: () -> Unit, onReload: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        FilledTonalButton(onClick = onAdd, contentPadding = PaddingValues(horizontal = 12.dp, vertical = 8.dp)) {
            Icon(Icons.Outlined.Add, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Add Collection")
        }
        OutlinedButton(onClick = onDelete, colors = ButtonDefaults.outlinedButtonColors()) {
            Icon(Icons.Outlined.Delete, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Delete")
        }
        OutlinedButton(onClick = onReload) {
            Icon(Icons.Outlined.Refresh, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Reload")
        }
    }
}

@Composable
private fun SidebarCard(content: @Composable () -> Unit) {
    Surface(tonalElevation = 0.dp, shape = MaterialTheme.shapes.medium, color = Color.Transparent) {
        Box(Modifier.fillMaxWidth().padding(4.dp)) { content() }
    }
}
data class CollectionDetails(
    val name: String,
    val shardCount: Int? = null,
    val configName: String? = null,
    val replicationFactor: Int? = null,
    val router: String? = null,
    val shards: List<Shard> = emptyList(),
)

data class Shard(val name: String)

@Composable
private fun KeyValRow(key: String, value: String) {
    Row(Modifier.fillMaxWidth().padding(vertical = 6.dp)) {
        Text("$key:", modifier = Modifier.width(180.dp), color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value, color = MaterialTheme.colorScheme.onSurface)
    }
}

@Composable
fun ShardsSimpleCollapsible(
    shards: Map<String, Shard>, // Map key = shard name
    modifier: Modifier = Modifier,
) {
    val shardNames = remember(shards) { shards.keys.sorted() }
    var expandedShard by remember(shards) { mutableStateOf<String?>(null) }

    Column(modifier.fillMaxWidth()) {
        shardNames.forEachIndexed { index, name ->
            val shard = shards[name]
            val isOpen = expandedShard == name
            val arrow = if (isOpen) "▼" else "▶"

            // Header row (clickable) — meta sits close to title, not at far edge
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable {
                        expandedShard = if (isOpen) null else name
                    }
                    .padding(vertical = 6.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text("$arrow Shard: $name", style = MaterialTheme.typography.titleSmall)
                Spacer(Modifier.width(12.dp))
                val meta = listOfNotNull(
                    shard?.state?.let { "state=$it" },
                    shard?.range?.let { "range=$it" },
                ).joinToString("   ")
                if (meta.isNotBlank()) {
                    Text(
                        meta,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }

            // Replicas (indented) — only when open
            if (isOpen && shard != null) {
                Spacer(Modifier.height(6.dp))
                Column(Modifier.padding(start = 20.dp)) {
                    val replicas = shard.replicas.entries.sortedBy { it.key }
                    if (replicas.isEmpty()) {
                        Text(
                            "No replicas",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    } else {
                        replicas.forEach { (replicaName, rep) ->
                            val isLeader = rep.leader?.equals("true", ignoreCase = true) == true
                            val bits = listOfNotNull(rep.type, rep.state).filter { it.isNotBlank() }.joinToString(" / ")
                            val suffix = if (bits.isNotEmpty()) "  ($bits)" else ""
                            Text(
                                text = "• ${if (isLeader) "★ " else ""}$replicaName$suffix",
                                style = MaterialTheme.typography.bodyMedium,
                            )
                        }
                    }
                }
            }

            if (index != shardNames.lastIndex) {
                Spacer(Modifier.height(8.dp))
                HorizontalDivider()
                Spacer(Modifier.height(8.dp))
            }
        }
    }
}
