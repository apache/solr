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

import io.ktor.client.engine.mock.respond
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.fullPath
import io.ktor.http.headersOf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.createMockEngine
import org.apache.solr.ui.domain.RequestHandlerOption
import org.apache.solr.ui.testHttpClient

class HttpIndexQueryRepositoryTest {

    @Test
    fun `lists collections from the collections endpoint`() = runTest {
        val engine = createMockEngine(
            { request ->
                assertEquals("/api/collections", request.url.fullPath)
                respond(
                    content = """{"collections":["films","books"]}""",
                    headers = jsonHeaders,
                )
            },
        )
        val httpClient = testHttpClient(engine)

        try {
            val result = HttpIndexQueryRepository(httpClient).listCollections()

            assertEquals(listOf("films", "books"), result.getOrThrow())
            assertEquals(1, engine.requestHistory.size)
        } finally {
            httpClient.close()
        }
    }

    @Test
    fun `lists request handlers with their class names sorted by name`() = runTest {
        val engine = createMockEngine(
            { request ->
                assertEquals("/api/collections/books/config/requestHandler", request.url.fullPath)
                respond(
                    content = """{"config":{"requestHandler":{"/update":{"class":"solr.UpdateRequestHandler"},"/select":{"class":"solr.SearchHandler"}}}}""",
                    headers = jsonHeaders,
                )
            },
        )
        val httpClient = testHttpClient(engine)

        try {
            val result = HttpIndexQueryRepository(httpClient).listRequestHandlers("books")

            assertEquals(
                listOf(
                    RequestHandlerOption(name = "/select", className = "solr.SearchHandler"),
                    RequestHandlerOption(name = "/update", className = "solr.UpdateRequestHandler"),
                ),
                result.getOrThrow(),
            )
            assertEquals(1, engine.requestHistory.size)
        } finally {
            httpClient.close()
        }
    }

    @Test
    fun `returns failure for a non-success response`() = runTest {
        val engine = createMockEngine(
            { _ -> respond(content = "{}", status = HttpStatusCode.ServiceUnavailable) },
        )
        val httpClient = testHttpClient(engine)

        try {
            val result = HttpIndexQueryRepository(httpClient).listCollections()

            assertTrue(result.isFailure)
        } finally {
            httpClient.close()
        }
    }

    private companion object {
        val jsonHeaders = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
    }
}
