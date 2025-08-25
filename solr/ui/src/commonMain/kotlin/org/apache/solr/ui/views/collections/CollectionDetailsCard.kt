package org.apache.solr.ui.views.collections

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Refresh
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedCard
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.collections.data.CollectionData

@Composable
fun CollectionDetailsCard(
    details: CollectionData,
    isLoading: Boolean,
    onDelete: (String) -> Unit,
    onReload: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val component = LocalCollectionsProvider.current
    var showDelete by remember { mutableStateOf(false) }
    val model by component.model.collectAsState()

    OutlinedCard(
        modifier = modifier,
        shape = MaterialTheme.shapes.medium,
        colors = CardDefaults.outlinedCardColors(),
    ) {
        // Header
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 10.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text("Collection: ${details.name}", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            // tiny badges
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                StatPill("${details.shardCount} shards")
                StatPill("RF ${details.replicationFactor}")
            }
            Spacer(Modifier.width(12.dp))
            if (isLoading) {
                CircularProgressIndicator(modifier = Modifier.size(16.dp), strokeWidth = 2.dp)
                Spacer(Modifier.width(8.dp))
            }
            OutlinedButton(onClick = { showDelete = true }, enabled = !isLoading, contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp)) {
                Icon(Icons.Outlined.Delete, null)
                Spacer(Modifier.width(6.dp))
                Text("Delete")
            }
            DeleteCollectionDialog(
                open = showDelete,
                collectionName = details.name, // or the name from your details pane
                onDismiss = { showDelete = false },
            )
            Spacer(Modifier.width(8.dp))
            OutlinedButton(onClick = onReload, enabled = !isLoading, contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp)) {
                Icon(Icons.Outlined.Refresh, null)
                Spacer(Modifier.width(6.dp))
                Text("Reload")
            }
        }

        HorizontalDivider()

        // Key/Value lines
        KVItem("Shard count", details.shardCount.toString())
        KVItem("configName", details.configName)
        KVItem("replicationFactor", details.replicationFactor.toString())
        KVItem("router", details.routerName)
        Spacer(Modifier.height(4.dp))
    }
}

@Composable
private fun KVItem(label: String, value: String) {
    ListItem(
        headlineContent = { Text(label) },
        trailingContent = { Text(value, style = MaterialTheme.typography.bodyLarge) },
        modifier = Modifier.padding(horizontal = 8.dp),
    )
}

@Composable
private fun StatPill(text: String) {
    Surface(
        shape = MaterialTheme.shapes.large,
        color = MaterialTheme.colorScheme.surfaceVariant,
        contentColor = MaterialTheme.colorScheme.onSurfaceVariant,
        tonalElevation = 0.dp,
    ) {
        Text(
            text,
            modifier = Modifier.padding(horizontal = 8.dp, vertical = 2.dp),
            style = MaterialTheme.typography.labelSmall,
        )
    }
}
