package org.apache.solr.ui.views.collections

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.views.theme.backgroundLight

@Composable
fun CollectionsListCard(
    names: List<String>,
    selected: String?,
    onSelect: (String) -> Unit,
    modifier: Modifier = Modifier,
) {
    LazyColumn(
        modifier = Modifier.width(200.dp),
    ) {
        items(names) { name ->
            val isSelected = name == selected
            Text(
                text = name,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onSelect(name) }
                    .padding(12.dp),
            )
        }
    }
}
