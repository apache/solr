package org.apache.solr.ui.views.collections.shards.replica

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Star
import androidx.compose.material3.Icon
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.collections.data.Replica

@Composable
fun ReplicaListItem(
    replicaName: String,
    rep: Replica,
    onDelete: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val isLeader = rep.leader?.equals("true", ignoreCase = true) == true
    val secondary = listOfNotNull(rep.type, rep.state)
        .filter { it.isNotBlank() }
        .joinToString(" • ")

    ListItem(
        // star sits right next to the name here
        headlineContent = {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(replicaName)
                if (isLeader) {
                    Spacer(Modifier.width(4.dp))
                    Icon(Icons.Outlined.Star, contentDescription = "Leader", tint = MaterialTheme.colorScheme.primary, modifier = Modifier.size(16.dp))
                }
            }
        },
        supportingContent = {
            Column {
                if (secondary.isNotEmpty()) Text(secondary)
                // optional 3rd line info
                val node = rep.nodeName ?: rep.baseUrl
                if (!node.isNullOrBlank()) {
                    Text(node, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }
        },
        trailingContent = {
            OutlinedButton(
                onClick = onDelete,
                contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp),
            ) {
                Icon(Icons.Outlined.Delete, contentDescription = null)
                Spacer(Modifier.width(6.dp))
                Text("Delete")
            }
        },
        modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp),
    )
}
