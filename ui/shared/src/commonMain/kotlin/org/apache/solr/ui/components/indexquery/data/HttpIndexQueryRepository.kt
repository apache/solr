/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.ui.components.indexquery.data

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.http.appendPathSegments
import io.ktor.http.isSuccess
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.solr.ui.domain.RequestHandlerOption

private fun isValidCollectionName(collection: String): Boolean =
    collection.isNotEmpty() &&
        collection.first() != '-' &&
        collection.all {
            it in 'A'..'Z' ||
                it in 'a'..'z' ||
                it in '0'..'9' ||
                it == '.' ||
                it == '_' ||
                it == '-'
        }

class HttpIndexQueryRepository(private val httpClient: HttpClient) : IndexQueryRepository {
    override suspend fun listCollections(): Result<List<String>> = requestResult {
        val response = httpClient.get("api/collections")
        when {
            response.status.isSuccess() -> Result.success(response.body<CollectionsResponse>().collections)
            else -> Result.failure(Exception("Unknown Error"))
        }
    }

    override suspend fun listRequestHandlers(collection: String): Result<List<RequestHandlerOption>> = requestResult {
        if (collection == "." || collection == ".." || !isValidCollectionName(collection)) {
            return@requestResult Result.failure(IllegalArgumentException("Invalid collection name"))
        }

        val response = httpClient.get {
            url {
                appendPathSegments(
                    "api",
                    "collections",
                    collection,
                    "config",
                    "requestHandlers",
                    encodeSlash = true,
                )
            }
        }
        when {
            response.status.isSuccess() -> {
                val handlers = response.body<RequestHandlersResponse>().config.requestHandlers
                Result.success(
                    handlers.map { (name, handler) -> RequestHandlerOption(name, handler.className) }.sortedBy { it.name },
                )
            }
            else -> Result.failure(Exception("Unknown Error"))
        }
    }
}

private suspend fun <T> requestResult(block: suspend () -> Result<T>): Result<T> = try {
    block()
} catch (e: CancellationException) {
    throw e
} catch (e: Exception) {
    Result.failure(e)
}

@Serializable
private data class CollectionsResponse(val collections: List<String> = emptyList())

@Serializable
private data class RequestHandlersResponse(val config: ConfigResponse)

@Serializable
private data class ConfigResponse(val requestHandlers: Map<String, RequestHandlerResponse> = emptyMap())

@Serializable
private data class RequestHandlerResponse(@SerialName("class") val className: String = "")
