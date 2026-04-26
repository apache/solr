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

package org.apache.solr.ui.components.configsets.data

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.domain.PickedFile

class HttpConfigsetsRepository(private val httpClient: HttpClient) : ConfigsetsRepository {
    override suspend fun createConfigset(name: String, baseConfigset: String?): Result<Configset> {
        val response = httpClient.post("api/configsets") {
            setBody(
                CreateConfigset(
                    name = name,
                    baseConfigSet = baseConfigset,
                ),
            )
        }
        return when {
            response.status.isSuccess() ->
                // Success result will not contain any information,
                // therefore, create the configset from input
                Result.success(Configset(name))
            else -> Result.failure(Exception("Unknown Error"))
        }
    }

    override suspend fun importConfigset(name: String, file: PickedFile): Result<Configset> {
        val response = httpClient.put("api/configsets/$name") {
            contentType(ContentType.parse("application/zip"))
            setBody(file.bytes)
        }
        return when {
            response.status.isSuccess() -> Result.success(Configset(name = name))
            else -> Result.failure(Exception("Unknown Error"))
        }
    }

    override suspend fun loadConfigsets(): Result<List<Configset>> {
        val response = httpClient.get("api/configsets")
        return when {
            response.status.isSuccess() -> {
                val data = response.body<ListConfigsets>()
                Result.success(data.configSets.map { Configset(name = it) })
            }
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }
}
