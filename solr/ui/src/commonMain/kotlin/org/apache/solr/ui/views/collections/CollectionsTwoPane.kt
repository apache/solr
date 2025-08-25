package org.apache.solr.ui.views.collections

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Add
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.RectangleShape
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.collections.data.CollectionData
import org.apache.solr.ui.views.collections.shards.ShardsMaterialCards

@Composable
fun CollectionsTwoPane(
    names: List<String>,
    selected: String?,
    details: CollectionData?,
    onSelect: (String) -> Unit,
    onAdd: () -> Unit,
    onDelete: (String) -> Unit,
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
                val d = details
                if (d == null) {
                    Text("Select a collection", style = MaterialTheme.typography.titleMedium)
                    Spacer(Modifier.height(8.dp))
                    HorizontalDivider()
                } else {
                    CollectionDetailsCard(
                        details = details,
                        isLoading = false, // or false if you don’t track it
                        onDelete = onDelete,
                        onReload = onReload,
                        modifier = Modifier.fillMaxWidth(),
                    )

                    Spacer(Modifier.height(16.dp))

                    ShardsMaterialCards(
                        shards = d.shards, // Map<String, Shard>
                        resetKey = details?.name, // resets expansion when switching collections
                    )
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
private fun SidebarCard(content: @Composable () -> Unit) {
    Surface(tonalElevation = 0.dp, shape = MaterialTheme.shapes.medium, color = Color.Transparent) {
        Box(Modifier.fillMaxWidth().padding(4.dp)) { content() }
    }
}
