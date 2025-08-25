package org.apache.solr.ui.components.collections.integration

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.parameter
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.apache.solr.ui.components.collections.data.ClusterStatusResponse
import org.apache.solr.ui.components.collections.data.CollectionsList
import org.apache.solr.ui.components.collections.data.ZkTree
import org.apache.solr.ui.components.collections.store.CollectionsStoreProvider

class HttpCollectionsStoreClient(
    private val httpClient: HttpClient,
) : CollectionsStoreProvider.Client {

    override suspend fun getCollections(): Result<CollectionsList> {
        val response = httpClient.get("api/collections")

        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun getClusterStatus(collectionName: String): Result<ClusterStatusResponse> {
        val response = httpClient.get("api/cluster?collection=$collectionName")
        return when {
            response.status.isSuccess() -> {
                Result.success(response.body())
            }
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun getZkTree(): Result<ZkTree> {
        val response = httpClient.get("solr/admin/zookeeper?path=%2Flive_nodes&wt=json")
        return when {
            response.status.isSuccess() -> {
                Result.success(response.body())
            }
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }
    override suspend fun addReplica(
        collectionName: String,
        nodeName: String?,
        shardName: String,
        type: String,
    ): Result<JsonObject> {
        val response = httpClient.post("api/collections/$collectionName/shards/$shardName/replicas") {
            contentType(ContentType.Application.Json)
            setBody(
                buildJsonObject {
                    nodeName?.let { put("node", it) }
                    put("type", type)
                },
            )
        }
        return when {
            response.status.isSuccess() -> {
                println(response.body<String>())
                Result.success(response.body())
            }
            else -> {
                Result.failure(Exception("Unknown Error"))
            }
            // TODO Add proper error handling
        }
    }

    override suspend fun deleteReplica(
        collectionName: String,
        shardName: String,
        replicaName: String,
    ): Result<JsonObject> {
        val response = httpClient.delete("api/collections/$collectionName/shards/$shardName/replicas/$replicaName")
        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun deleteCollection(
        collectionName: String,
    ): Result<JsonObject> {
        val response = httpClient.delete("api/collections/$collectionName")
        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }
}
