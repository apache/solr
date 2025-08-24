package org.apache.solr.ui.components.collections.integration

import org.apache.solr.ui.components.collections.CollectionsComponent
import org.apache.solr.ui.components.collections.store.CollectionsStore

internal val collectionsStateToModel: (CollectionsStore.State) -> CollectionsComponent.Model = {
    CollectionsComponent.Model(
        collectionsList = it.collectionsList,
        selectedCollectionData = it.selectedCollectionData,
    )
}
