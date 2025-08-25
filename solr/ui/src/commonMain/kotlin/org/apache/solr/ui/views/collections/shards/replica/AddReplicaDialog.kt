package org.apache.solr.ui.views.collections.shards.replica

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.flow.map
import org.apache.solr.ui.views.collections.LocalCollectionsProvider

enum class ReplicaType(val solr: String) { NRT("NRT"), TLOG("TLOG"), PULL("PULL") }

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun AddReplicaDialog(
    visible: Boolean,
    shardName: String,
    onDismiss: () -> Unit,
    onConfirm: (node: String?, type: String) -> Unit, // keep if you want parent to handle
) {
    if (!visible) return

    val component = LocalCollectionsProvider.current
    // read just the liveNodes slice
    val liveNodes by component.model
        .map { it.liveNodesData } // or .liveNodes if that’s your field name
        .collectAsState(initial = emptyList())
    // fetch nodes when the dialog opens
    LaunchedEffect(visible) {
        if (visible) component.fetchLiveNodesData()
    }
    var nodeExpanded by remember { mutableStateOf(false) }
    var typeExpanded by remember { mutableStateOf(false) }
    var selectedNode by remember { mutableStateOf<String?>(null) } // null => no specified node
    var selectedType by remember { mutableStateOf(ReplicaType.NRT) }

    AlertDialog(
        onDismissRequest = onDismiss,
        confirmButton = {
            Button(onClick = { onConfirm(selectedNode, selectedType.solr) }) { Text("Create Replica") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
        title = { Text("Add replica to $shardName") },
        text = {
            Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                // Node dropdown (optional)
                ExposedDropdownMenuBox(expanded = nodeExpanded, onExpandedChange = { nodeExpanded = it }) {
                    OutlinedTextField(
                        readOnly = true,
                        value = selectedNode ?: "No specified node",
                        onValueChange = {},
                        label = { Text("Node") },
                        trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = nodeExpanded) },
                        modifier = Modifier.menuAnchor().fillMaxWidth(),
                    )
                    ExposedDropdownMenu(expanded = nodeExpanded, onDismissRequest = { nodeExpanded = false }) {
                        DropdownMenuItem(
                            text = { Text("No specified node") },
                            onClick = {
                                selectedNode = null
                                nodeExpanded = false
                            },
                        )
                        liveNodes.forEach { n ->
                            DropdownMenuItem(text = { Text(n) }, onClick = {
                                selectedNode = n
                                nodeExpanded = false
                            })
                        }
                    }
                }

                // Type dropdown
                ExposedDropdownMenuBox(expanded = typeExpanded, onExpandedChange = { typeExpanded = it }) {
                    OutlinedTextField(
                        readOnly = true,
                        value = selectedType.name,
                        onValueChange = {},
                        label = { Text("Type") },
                        trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = typeExpanded) },
                        modifier = Modifier.menuAnchor().fillMaxWidth(),
                    )
                    ExposedDropdownMenu(expanded = typeExpanded, onDismissRequest = { typeExpanded = false }) {
                        ReplicaType.entries.forEach { t ->
                            DropdownMenuItem(text = { Text(t.name) }, onClick = {
                                selectedType = t
                                typeExpanded = false
                            })
                        }
                    }
                }
            }
        },
    )
}
