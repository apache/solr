package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class Jmx(
    val classpath: String = "",
    val commandLineArgs: List<String> = emptyList(),
    val startTime: String = "",
    val upTimeMS: Long = 0,
)
