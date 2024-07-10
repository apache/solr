package org.apache.solr.composeui.components.environment.integration

import org.apache.solr.composeui.components.environment.EnvironmentComponent
import org.apache.solr.composeui.components.environment.store.EnvironmentStore

internal val environmentStateToModel: (EnvironmentStore.State) -> EnvironmentComponent.Model = {
    EnvironmentComponent.Model(
        mode = it.mode,
        zkHost = it.zkHost,
        solrHome = it.solrHome,
        coreRoot = it.coreRoot,
        lucene = it.lucene,
        jvm = it.jvm,
        security = it.security,
        system = it.system,
        node = it.node,
        javaProperties = it.javaProperties,
    )
}
