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
import io.ktor.http.isSuccess
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.solr.ui.domain.RequestHandlerOption

class HttpIndexQueryRepository(private val httpClient: HttpClient) : IndexQueryRepository {
    override suspend fun listCollections(): Result<List<String>> {
        val response = httpClient.get("api/collections")
        return when {
            response.status.isSuccess() -> Result.success(response.body<CollectionsResponse>().collections)
            else -> Result.failure(Exception("Unknown Error"))
        }
    }

    override suspend fun listRequestHandlers(collection: String): Result<List<RequestHandlerOption>> {
        val response = httpClient.get("api/collections/$collection/config/requestHandler")
        return when {
            response.status.isSuccess() -> {
                val handlers = response.body<RequestHandlersResponse>().config.requestHandler
                Result.success(
                    handlers.map { (name, handler) -> RequestHandlerOption(name, handler.className) }.sortedBy { it.name },
                )
            }
            else -> Result.failure(Exception("Unknown Error"))
        }
    }
}

@Serializable
private class CollectionsResponse(val collections: List<String> = emptyList())

@Serializable
private class RequestHandlersResponse(val config: ConfigResponse)

@Serializable
private class ConfigResponse(val requestHandler: Map<String, RequestHandlerResponse> = emptyMap())

@Serializable
private class RequestHandlerResponse(@SerialName("class") val className: String = "")
