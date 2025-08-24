package org.apache.solr.ui.components.collections.integration

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.http.isSuccess
import org.apache.solr.ui.components.collections.data.ClusterStatusResponse
import org.apache.solr.ui.components.collections.data.CollectionsList
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
}
