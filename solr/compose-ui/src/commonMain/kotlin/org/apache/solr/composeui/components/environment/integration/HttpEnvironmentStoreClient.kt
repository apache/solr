package org.apache.solr.composeui.components.environment.integration

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.http.isSuccess
import org.apache.solr.composeui.components.environment.data.JavaPropertiesResponse
import org.apache.solr.composeui.components.environment.data.JavaProperty
import org.apache.solr.composeui.components.environment.data.SystemData
import org.apache.solr.composeui.components.environment.store.EnvironmentStoreProvider

class HttpEnvironmentStoreClient(
    private val httpClient: HttpClient,
) : EnvironmentStoreProvider.Client {

    override suspend fun getSystemData(): Result<SystemData> {
        val response = httpClient.get("api/node/system")

        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun getJavaProperties(): Result<List<JavaProperty>> {
        val response = httpClient.get("api/node/properties")

        return when {
            response.status.isSuccess() -> {
                val javaProperties = response.body<JavaPropertiesResponse>()
                    .properties
                    .map { (key, value) -> key to value }

                Result.success(javaProperties)
            }
            else -> Result.failure(Exception("Unknown error"))
            // TODO Add proper error handling
        }
    }
}
