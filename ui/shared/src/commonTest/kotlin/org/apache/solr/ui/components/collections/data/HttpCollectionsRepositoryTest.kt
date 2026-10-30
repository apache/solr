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
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.domain.CreateCollectionInput
import org.apache.solr.ui.domain.CreateCollectionResult
import kotlin.test.Test
import kotlin.test.assertEquals

class HttpCollectionsRepositoryTest {

    private val input = CreateCollectionInput(
        name = "films2",
        configset = "techproducts",
        shards = 2,
        nrtReplicas = 3,
        tlogReplicas = 1,
        pullReplicas = 0,
        router = "implicit",
        routeField = "id",
        async = "req-1",
        properties = mapOf("maxShardsPerNode" to "2"),
    )

    private fun clientFor(engine: MockEngine): HttpClient = HttpClient(engine) {
        defaultRequest { url("http://localhost/") }
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    }

    @Test
    fun testPostsTheCollectionAsARequestBody() = runTest {
        var seen: CreateCollectionRequest? = null
        var method: HttpMethod? = null
        var path: String? = null
        val engine = createMockEngine(
            { request ->
                method = request.method
                path = request.url.encodedPath
                seen = Json.decodeFromString(
                    CreateCollectionRequest.serializer(),
                    (request.body as TextContent).text,
                )
                respond("{}", HttpStatusCode.OK, headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()))
            },
        )

        val result = HttpCollectionsRepository(clientFor(engine)).createCollection(input)

        assertEquals(CreateCollectionResult.Created, result)
        assertEquals(HttpMethod.Post, method)
        assertEquals("/api/collections", path)
        assertEquals(
            CreateCollectionRequest(
                name = "films2",
                config = "techproducts",
                numShards = 2,
                nrtReplicas = 3,
                tlogReplicas = 1,
                pullReplicas = 0,
                router = RouterRequest(name = "implicit", field = "id"),
                async = "req-1",
                properties = mapOf("maxShardsPerNode" to "2"),
            ),
            seen,
        )
    }

    @Test
    fun testMapsTheDuplicateNameErrorToNameExists() = runTest {
        val engine = createMockEngine(
            { _ ->
                respond(
                    """{"responseHeader":{"status":400},"error":{"msg":"collection already exists: films2","code":400}}""",
                    HttpStatusCode.BadRequest,
                    headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
                )
            },
        )

        val result = HttpCollectionsRepository(clientFor(engine)).createCollection(input)

        assertEquals(CreateCollectionResult.NameExists, result)
    }

    @Test
    fun testAliasConflictIsNotMappedToNameExists() = runTest {
        val engine = createMockEngine(
            { _ ->
                respond(
                    """{"error":{"msg":"collection alias already exists: films2","code":400}}""",
                    HttpStatusCode.BadRequest,
                    headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
                )
            },
        )

        val result = HttpCollectionsRepository(clientFor(engine)).createCollection(input)

        assertEquals(CreateCollectionResult.Failed("collection alias already exists: films2"), result)
    }

    @Test
    fun testKeepsTheServerMessageForOtherFailures() = runTest {
        val engine = createMockEngine(
            { _ ->
                respond(
                    """{"error":{"msg":"Invalid collection: [a b]","code":400}}""",
                    HttpStatusCode.BadRequest,
                    headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
                )
            },
        )

        val result = HttpCollectionsRepository(clientFor(engine)).createCollection(input)

        assertEquals(CreateCollectionResult.Failed("Invalid collection: [a b]"), result)
    }

    @Test
    fun testFailureWithoutAJsonBodyHasNoMessage() = runTest {
        val engine = createMockEngine(
            { _ ->
                respond("<html>Bad gateway</html>", HttpStatusCode.BadGateway, headersOf(HttpHeaders.ContentType, ContentType.Text.Html.toString()))
            },
        )

        val result = HttpCollectionsRepository(clientFor(engine)).createCollection(input)

        assertEquals(CreateCollectionResult.Failed(null), result)
    }
}
