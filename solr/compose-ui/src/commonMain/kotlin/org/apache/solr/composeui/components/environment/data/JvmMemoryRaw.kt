package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class JvmMemoryRaw(
    val free: Int = 0,
    val total: Int = 0,
    val max: Int = 0,
    val used: Int = 0,
    @SerialName("used%")
    val usedPercentage: Double = 0.0,
)