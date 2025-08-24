package org.apache.solr.ui.components.collections.data

data class CollectionData(
    val name: String = "",
    val configName: String = "",
    val replicationFactor: Int = 0,
    val routerName: String = "",
    val shards: Map<String, Shard> = emptyMap(),
    val shardCount: Int = 0,
    val router: String = "",
)
