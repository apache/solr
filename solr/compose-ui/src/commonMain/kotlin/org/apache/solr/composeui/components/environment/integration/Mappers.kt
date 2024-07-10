package org.apache.solr.composeui.components.environment.integration

import org.apache.solr.composeui.components.environment.EnvironmentComponent
import org.apache.solr.composeui.components.environment.store.EnvironmentStore

internal val environmentStateToModel: (EnvironmentStore.State) -> EnvironmentComponent.Model = {
    EnvironmentComponent.Model(
        versions = it.lucene,
        jvm = it.jvm,
        javaProperties = it.javaProperties,
    )
}
