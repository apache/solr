package org.apache.solr.ui.views.collections


import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Modifier
import org.apache.solr.ui.components.collections.CollectionsComponent
import org.apache.solr.ui.components.collections.store.CollectionsStore

val LocalCollectionsProvider = staticCompositionLocalOf<CollectionsComponent> {
    error("CollectionsComponent not provided")
}

@Composable
fun CollectionsContent(
    component: CollectionsComponent,
    modifier: Modifier = Modifier,
) {
    CompositionLocalProvider(LocalCollectionsProvider provides component) {
        val model by component.model.collectAsState()
        val snack = remember { SnackbarHostState() }

        // one-off events
        LaunchedEffect(component) {
            component.labels.collect { label ->
                when (label) {
                    is CollectionsStore.Label.ReplicaAdded   -> snack.showSnackbar("Replica added to ${label.shard}")
                    is CollectionsStore.Label.ReplicaDeleted -> snack.showSnackbar("Replica ${label.replica} deleted")
                    is CollectionsStore.Label.Error          -> snack.showSnackbar(label.message)
                    is CollectionsStore.Label.CollectionDeleted -> snack.showSnackbar("Collection ${label.name} deleted")
                }
            }
        }
        Scaffold(snackbarHost = { SnackbarHost(snack) }) {
            CollectionsTwoPane(
                names = model.collectionsList.collectionsList,
                selected = "Lucid",
                details = model.selectedCollectionData,
                onSelect = { name ->
                    component.selectCollection(name)
                },
                onAdd = {},
                onDelete = { name ->
                    component.deleteCollection(name)
                },
                onReload = {},
            )
        }
    }
}
