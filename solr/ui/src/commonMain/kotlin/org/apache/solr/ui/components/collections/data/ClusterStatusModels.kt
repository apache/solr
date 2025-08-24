package org.apache.solr.ui.components.collections.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class ClusterStatusResponse(
    val responseHeader: ResponseHeader? = null,
    val cluster: Cluster,
)

@Serializable
data class ResponseHeader(
    val status: Int = 0,
    val QTime: Int = 0,
)

@Serializable
data class Cluster(
    @SerialName("live_nodes") val liveNodes: List<String> = emptyList(),
    val collections: Map<String, CollectionInfo> = emptyMap(),
    val properties: Map<String, JsonElement> = emptyMap(),
    val roles: Map<String, JsonElement> = emptyMap(),
)

@Serializable
data class CollectionInfo(
    // Strings in your sample JSON:
    val pullReplicas: String? = null,
    val tlogReplicas: String? = null,

    // Numbers in your sample JSON:
    val nrtReplicas: Int? = null,
    val replicationFactor: Int? = null,

    val configName: String? = null,
    val router: Router? = null,
    val shards: Map<String, Shard> = emptyMap(),
    val health: String? = null,
    val znodeVersion: Int? = null,
    val creationTimeMillis: Long? = null,
) {
    fun toCollectionData(name: String) = CollectionData(
        name = name,
        configName = configName.orEmpty(),
        replicationFactor = replicationFactor ?: 0,
        routerName = router?.name.orEmpty(),
        router = router?.name.orEmpty(),
        shards = shards,
        shardCount = shards.size,
    )
}

@Serializable
data class Router(
    val name: String? = null,
)

@Serializable
data class Shard(
    val range: String? = null,
    val replicas: Map<String, Replica> = emptyMap(),
    val state: String? = null,
    val health: String? = null,
)

@Serializable
data class Replica(
    val core: String? = null,
    @SerialName("node_name") val nodeName: String? = null,
    val type: String? = null,
    val state: String? = null,
    // “true”/“false” are strings in your sample JSON
    val leader: String? = null,
    @SerialName("force_set_state") val forceSetState: String? = null,
    @SerialName("base_url") val baseUrl: String? = null,
)

internal fun ClusterStatusResponse.collectionInfo(name: String): CollectionInfo? = cluster.collections[name]

internal fun ClusterStatusResponse.toCollectionDataOrNull(name: String): CollectionData? = collectionInfo(name)?.toCollectionData(name)
