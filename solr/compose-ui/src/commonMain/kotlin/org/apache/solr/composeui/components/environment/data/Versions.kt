package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class Versions(
    @SerialName("solr-spec-version")
    val solrSpecVersion: String = "",
    @SerialName("solr-impl-version")
    val solrImplVersion: String = "",
    @SerialName("lucene-spec-version")
    val luceneSpecVersion: String = "",
    @SerialName("lucene-impl-version")
    val luceneImplVersion: String = "",
)
