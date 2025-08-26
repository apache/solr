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
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.RectangleShape
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.collections.CollectionsComponent
import org.apache.solr.ui.components.collections.store.CollectionsStore
import org.apache.solr.ui.views.collections.shards.ShardsMaterialCards

@Composable
fun CollectionsContent(
    component: CollectionsComponent,
    modifier: Modifier = Modifier,
) {
    val model by component.model.collectAsState()
    val snack = remember { SnackbarHostState() }
    var showCreateDialog by remember { mutableStateOf(false) }

    // one-off events
    LaunchedEffect(component) {
        component.labels.collect { label ->
            when (label) {
                is CollectionsStore.Label.ReplicaAdded -> snack.showSnackbar("Replica added to ${label.shard}")
                is CollectionsStore.Label.ReplicaDeleted -> snack.showSnackbar("Replica ${label.replica} deleted")
                is CollectionsStore.Label.Error -> snack.showSnackbar(label.message)
                is CollectionsStore.Label.CollectionDeleted -> snack.showSnackbar("Collection ${label.name} deleted")
                is CollectionsStore.Label.CollectionReloaded -> snack.showSnackbar("Collection ${label.name} reloaded")
                is CollectionsStore.Label.CollectionCreated -> snack.showSnackbar("Collection ${label.name} created")
            }
        }
    }
    Scaffold(snackbarHost = { SnackbarHost(snack) }) {
        // Dialog
        CreateCollectionDialog(
            visible = showCreateDialog,
            configSets = model.configSets?.names ?: emptyList(),
            onFetchConfigSets = { component.fetchConfigSets() },
            onDismiss = { showCreateDialog = false },
            onConfirm = { name, numShards, replicas, configSet ->
                component.createCollection(name, numShards, replicas, configSet)
                showCreateDialog = false
            },
        )

        Row(Modifier.fillMaxWidth().padding(12.dp)) {
            // Left: toolbar + sidebar (fixed width)
            Column(
                Modifier.width(240.dp).border(
                    BorderStroke(0.dp, Color.Transparent), // keep base
                    shape = RectangleShape,
                ),
            ) {
                AddCollectionButton(onAdd = { showCreateDialog = true }, modifier = Modifier.fillMaxWidth().padding(12.dp))
                Spacer(Modifier.height(12.dp))
                SidebarCard {
                    CollectionsListCard(
                        names = model.collectionsList.collectionsList,
                        selected = model.selectedCollectionData?.name,
                        onSelect = { name -> component.selectCollection(name) },
                    )
                }
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
                    val d = model.selectedCollectionData
                    if (d == null) {
                        Text("Select a collection", style = MaterialTheme.typography.titleMedium)
                        Spacer(Modifier.height(8.dp))
                        HorizontalDivider()
                    } else {
                        CollectionDetailsCard(
                            details = d,
                            isLoading = false, // TODO wire a real loading state if needed
                            onDelete = { name -> component.deleteCollection(name) },
                            onReload = { name -> component.reloadCollection(name) },
                            modifier = Modifier.fillMaxWidth(),
                        )

                        Spacer(Modifier.height(16.dp))

                        ShardsMaterialCards(
                            shards = d.shards,
                            resetKey = d.name,
                            liveNodes = model.liveNodesData,
                            onFetchLiveNodes = { component.fetchLiveNodesData() },
                            onAddReplica = { shardName, node, type -> component.addReplica(node, shardName, type) },
                            onDeleteReplica = { shardName, replica -> component.deleteReplica(shardName, replica) },
                        )
                    }
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
    OutlinedButton(onClick = onAdd, modifier = modifier) {
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
