package org.apache.solr.composeui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class JvmData(
    val version: String = "",
    val name: String = "",
    val spec: JavaRuntimeInfo = JavaRuntimeInfo(),
    val jre: JavaRuntimeInfo = JavaRuntimeInfo(),
    val vm: JavaRuntimeInfo = JavaRuntimeInfo(),
    val processors: Int = 0,
    val memory: JvmMemory = JvmMemory(),
    val jmx: Jmx = Jmx(),
)
