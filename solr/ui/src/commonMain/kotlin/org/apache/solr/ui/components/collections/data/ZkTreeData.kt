package org.apache.solr.ui.components.collections.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ZkTree(
    val tree: List<ZkNode> = emptyList(),
)

fun ZkTree.liveNodes(): List<String> {
    val root = tree.firstOrNull { it.text == "/live_nodes" } ?: return emptyList()
    return root.children.map { it.text }
}

@Serializable
data class ZkNode(
    val text: String = "",
    @SerialName("a_attr") val aAttr: AAttr? = null,
    val children: List<ZkNode> = emptyList(),
    val ephemeral: Boolean? = null,
    val version: Int? = null,
)

@Serializable
data class AAttr(
    val href: String? = null,
)
