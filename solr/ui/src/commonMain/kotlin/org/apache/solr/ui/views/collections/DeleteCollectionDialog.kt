package org.apache.solr.ui.views.collections

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.AssistChip
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun DeleteCollectionDialog(
    open: Boolean,
    collectionName: String,
    onDismiss: () -> Unit,
) {
    if (!open) return

    val component = LocalCollectionsProvider.current

    var typed by rememberSaveable { mutableStateOf("") }
    val matches = typed.trim() == collectionName // require exact match like the old UI

    AlertDialog(
        onDismissRequest = onDismiss,
        confirmButton = {
            Button(
                enabled = matches,
                colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error),
                onClick = {
                    component.deleteCollection(
                        name = collectionName,
                    )
                    onDismiss()
                },
            ) { Text("Delete") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
        title = { Text("Delete collection") },
        text = {
            Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                Text(
                    "Please type the collection name to confirm deletion:",
                    style = MaterialTheme.typography.bodyMedium,
                )

                // The name being confirmed (helps avoid typos)
                AssistChip(
                    onClick = {},
                    label = { Text(collectionName, maxLines = 1, overflow = TextOverflow.Ellipsis) },
                )

                OutlinedTextField(
                    value = typed,
                    onValueChange = { typed = it },
                    label = { Text("Collection") },
                    singleLine = true,
                    isError = typed.isNotEmpty() && !matches,
                    supportingText = {
                        if (!matches && typed.isNotEmpty()) {
                            Text("Must exactly match “$collectionName”.")
                        }
                    },
                    keyboardOptions = KeyboardOptions(imeAction = ImeAction.Done),
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        },
    )
}
