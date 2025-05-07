package org.apache.solr.ui.views.auth

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.auth.UnauthenticatedComponent
import org.apache.solr.ui.views.components.SolrButton

/**
 * The user authentication content is the composable that will check and display
 * the available authentication options to the user.
 */
@Composable
fun UserAuthenticationContent(
    component: UnauthenticatedComponent,
    modifier: Modifier = Modifier,
) = Column(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    Text(text = "Authentication not implemented yet")

    SolrButton(onClick = component::onAbort) {
        Text(text = "Go Back")
    }
}
