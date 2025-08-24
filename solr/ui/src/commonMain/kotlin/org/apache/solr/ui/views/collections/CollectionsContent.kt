package org.apache.solr.ui.views.collections

import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import org.apache.solr.ui.components.collections.CollectionsComponent

@Composable
fun CollectionsContent(
    component: CollectionsComponent,
    modifier: Modifier = Modifier,
) {
    val model by component.model.collectAsState()

    CollectionsTwoPane(
        names = model.collectionsList.collectionsList,
        selected = "Lucid",
        details = model.selectedCollectionData,
        onSelect = { name ->
            component.selectCollection(name)
        },
        onAdd = {},
        onDelete = {},
        onReload = {},
    )
}
