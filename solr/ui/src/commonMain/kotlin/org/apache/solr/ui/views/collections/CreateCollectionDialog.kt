package org.apache.solr.ui.views.collections

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.KeyboardOptions
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
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun CreateCollectionDialog(
    visible: Boolean,
    configSets: List<String>,
    onFetchConfigSets: () -> Unit,
    onDismiss: () -> Unit,
    onConfirm: (name: String, numShards: Int, replicas: Int, configSet: String) -> Unit,
) {
    if (!visible) return

    val fetch by rememberUpdatedState(onFetchConfigSets)
    LaunchedEffect(visible) {
        if (visible) fetch()
    }

    var name by remember { mutableStateOf("") }
    var shardsText by remember { mutableStateOf("1") }
    var replicasText by remember { mutableStateOf("1") }

    var expanded by remember { mutableStateOf(false) }
    var selectedConfig by remember { mutableStateOf<String?>(null) }

    val shards = shardsText.toIntOrNull() ?: -1
    val replicas = replicasText.toIntOrNull() ?: -1

    val canCreate = name.isNotBlank() && shards >= 1 && replicas >= 1 && !selectedConfig.isNullOrBlank()

    AlertDialog(
        onDismissRequest = onDismiss,
        confirmButton = {
            Button(onClick = {
                selectedConfig?.let { cfg ->
                    onConfirm(name.trim(), shards, replicas, cfg)
                }
            }, enabled = canCreate) { Text("Create Collection") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
        title = { Text("Create Collection") },
        text = {
            Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                OutlinedTextField(
                    value = name,
                    onValueChange = { name = it },
                    label = { Text("Collection Name") },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth(),
                )

                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    OutlinedTextField(
                        value = shardsText,
                        onValueChange = { v -> if (v.all { it.isDigit() } || v.isEmpty()) shardsText = v },
                        label = { Text("Num Shards") },
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number, imeAction = ImeAction.Next),
                        modifier = Modifier.weight(1f),
                    )
                    OutlinedTextField(
                        value = replicasText,
                        onValueChange = { v -> if (v.all { it.isDigit() } || v.isEmpty()) replicasText = v },
                        label = { Text("Replicas per Shard") },
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number, imeAction = ImeAction.Done),
                        modifier = Modifier.weight(1f),
                    )
                }

                ExposedDropdownMenuBox(expanded = expanded, onExpandedChange = { expanded = it }) {
                    OutlinedTextField(
                        readOnly = true,
                        value = selectedConfig ?: "Select a ConfigSet",
                        onValueChange = {},
                        label = { Text("ConfigSet") },
                        trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
                        modifier = Modifier.menuAnchor().fillMaxWidth(),
                    )
                    ExposedDropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
                        configSets.forEach { cfg ->
                            DropdownMenuItem(text = { Text(cfg) }, onClick = {
                                selectedConfig = cfg
                                expanded = false
                            })
                        }
                    }
                }
            }
        },
        modifier = Modifier.padding(top = 4.dp),
    )
}
