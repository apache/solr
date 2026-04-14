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

package org.apache.solr.ui.components.configsets.integration

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.delete
import io.ktor.client.request.forms.FormPart
import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.request.forms.formData
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.writeFully
import org.apache.solr.ui.components.configsets.data.CreateConfigset
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.store.ConfigsetsStoreProvider
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStoreProvider
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStoreProvider
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.domain.PickedFile

/**
 * Implementation of any configset-related client interface of stores that makes use
 * of a preconfigured HTTP client for accessing the Solr API.
 *
 * @property httpClient HTTP client to use for accessing the API. The client has to be
 * configured with a default request that includes the host, port and schema. The client
 * should also include the necessary authentication data if authentication / authorization
 * is enabled.
 */
internal class HttpConfigsetsStoreClient(
    private val httpClient: HttpClient,
) : ConfigsetsStoreProvider.Client,
    CreateConfigsetStoreProvider.Client,
    ImportConfigsetStoreProvider.Client {

    override suspend fun fetchConfigSets(): Result<ListConfigsets> {
        val response = httpClient.get("api/configsets")
        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun deleteConfigset(configsetName: String): Result<Unit> {
        val response = httpClient.delete("api/configsets/$configsetName")
        return when {
            response.status.isSuccess() -> Result.success(Unit)
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }

    override suspend fun createConfigsetByName(
        configsetName: String,
        baseConfigset: String?,
    ): Result<Configset> {
        val response = httpClient.post("api/configsets") {
            setBody(
                CreateConfigset(
                    name = configsetName,
                    baseConfigSet = baseConfigset,
                ),
            )
        }
        return when {
            response.status.isSuccess() ->
                // Success result will not contain any information,
                // therefore, create the configset from input
                Result.success(Configset(name = configsetName))
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
}
