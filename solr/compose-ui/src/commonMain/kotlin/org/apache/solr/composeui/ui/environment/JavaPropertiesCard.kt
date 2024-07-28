package org.apache.solr.composeui.ui.environment

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.surfaceColorAtElevation
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.composeui.components.environment.data.JavaProperty
import org.apache.solr.composeui.ui.components.SolrCard
import org.apache.solr.composeui.ui.theme.SolrTheme

/**
 * Composable card that displays the provided java properties.
 *
 * @param properties List of java properties to display.
 * @param modifier Modifier to apply to the root composable.
 */
@Composable
internal fun JavaPropertiesCard(
    properties: List<JavaProperty>,
    modifier: Modifier = Modifier,
) = SolrCard(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp)
) {
    Text(
        text = "Java Properties",
        style = MaterialTheme.typography.headlineSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Column(
        modifier = Modifier.fillMaxWidth()
            .border(BorderStroke(1.dp, MaterialTheme.colorScheme.outlineVariant))
    ) {
        properties.forEachIndexed { index, property ->
            JavaPropertyEntry(
                property = property,
                isOdd = index % 2 == 0,
            )
        }
    }
}

/**
 * Composable of a single java property that can be used in lists.
 *
 * @param property Java property to display.
 * @param modifier Modifier to apply to the composable.
 * @param isOdd Whether the list entry has an odd number. This affects the background color
 * for a better separation of properties in long lists.
 */
@Composable
private fun JavaPropertyEntry(
    property: JavaProperty,
    modifier: Modifier = Modifier,
    isOdd: Boolean = false,
) = Row(
    modifier = modifier.background(
        MaterialTheme.colorScheme.surfaceColorAtElevation(
            if (isOdd) 1.dp else 0.dp,
        )
    ).padding(horizontal = 8.dp, vertical = 4.dp),
) {
    Text(
        modifier = Modifier.weight(1f),
        text = property.first,
        style = SolrTheme.typography.codeLarge,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Text(
        modifier = Modifier.weight(1f),
        text = property.second,
        style = SolrTheme.typography.codeLarge,
        color = MaterialTheme.colorScheme.onSurface,
    )
}
