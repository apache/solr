package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class SecurityConfig(
    val tls: Boolean = false,
)