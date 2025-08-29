package org.apache.solr.ui.views.configsets

import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.configsets.data.Configset

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ConfigsetsDropdown(
    selectedConfigSet: String,
    selectConfigset: (String) -> Unit,
    availableConfigsets: List<Configset>,
    modifier: Modifier = Modifier,
) {
    var expanded by remember { mutableStateOf(false) }
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        ExposedDropdownMenuBox(
            expanded = expanded,
            onExpandedChange = { expanded = !expanded },
            modifier = Modifier.widthIn(min = 256.dp).weight(1f),
        ) {
            OutlinedTextField(
                value = selectedConfigSet,
                onValueChange = {},
                readOnly = true,
                label = { Text("Configset") },
                trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded) },
                modifier = Modifier.menuAnchor().fillMaxWidth(),
            )
            ExposedDropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
                availableConfigsets.forEach { configset ->
                    DropdownMenuItem(
                        text = { Text(configset.name) },
                        onClick = {
                            selectConfigset(configset.name)
                            expanded = false
                        },
                    )
                }
            }
        }
    }
}
