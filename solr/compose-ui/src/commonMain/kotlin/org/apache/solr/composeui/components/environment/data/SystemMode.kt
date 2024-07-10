package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
enum class SystemMode {
    Unknown,
    @SerialName("solrcloud")
    SolrCloud,
    @SerialName("standalone")
    Standalone,
}
