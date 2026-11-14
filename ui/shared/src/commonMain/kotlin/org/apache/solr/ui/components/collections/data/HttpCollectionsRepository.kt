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

package org.apache.solr.ui.components.collections.data

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import org.apache.solr.ui.components.collections.repository.CollectionsRepository
import org.apache.solr.ui.domain.CreateCollectionInput
import org.apache.solr.ui.domain.CreateCollectionResult

private const val NAME_EXISTS_MESSAGE_PREFIX = "collection already exists"

class HttpCollectionsRepository(private val httpClient: HttpClient) : CollectionsRepository {
    override suspend fun createCollection(input: CreateCollectionInput): CreateCollectionResult {
        val response = httpClient.post("api/collections") {
            contentType(ContentType.Application.Json)
            setBody(input.toRequest())
        }
        if (response.status.isSuccess()) {
            return CreateCollectionResult.Created
        }
        val message = runCatching { response.body<SolrErrorResponse>().error?.msg }.getOrNull()
        return if (
            response.status == HttpStatusCode.BadRequest &&
            message?.startsWith(NAME_EXISTS_MESSAGE_PREFIX) == true
        ) {
            CreateCollectionResult.NameExists
        } else {
            CreateCollectionResult.Failed(message)
        }
    }
}

private fun CreateCollectionInput.toRequest() = CreateCollectionRequest(
    name = name,
    config = configset,
    numShards = shards,
    nrtReplicas = nrtReplicas,
    tlogReplicas = tlogReplicas,
    pullReplicas = pullReplicas,
    router = RouterRequest(name = router, field = routeField),
    async = async,
    properties = properties,
)
