package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Response class of the `java-properties` API endpoint.
 *
 * @property properties List if key-value pairs / Map that represent java properties.
 */
@Serializable
data class JavaPropertiesResponse(
    @SerialName("system.properties")
    val properties: Map<String, String>,
)
