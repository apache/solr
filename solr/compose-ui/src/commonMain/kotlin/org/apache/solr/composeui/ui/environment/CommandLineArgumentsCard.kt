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
import org.apache.solr.composeui.ui.theme.Fonts

@Composable
internal fun CommandLineArgumentsCard(
    modifier: Modifier = Modifier,
    arguments: List<String>,
) = SolrCard(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp)
) {
    Text(
        text = "Command Line Arguments",
        style = MaterialTheme.typography.headlineSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Column(
        modifier = Modifier.fillMaxWidth()
            .border(BorderStroke(1.dp, MaterialTheme.colorScheme.outlineVariant))
    ) {
        arguments.forEachIndexed { index, argument ->
            Text(
                modifier = Modifier.fillMaxWidth()
                    .background(
                        MaterialTheme.colorScheme.surfaceColorAtElevation(
                            if(index % 2 == 0) 1.dp else 0.dp,
                        )
                    ).padding(horizontal = 8.dp, vertical = 4.dp),
                text = argument,
                fontFamily = Fonts.firaCode(),
                style = MaterialTheme.typography.labelLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}
