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

package org.apache.solr.ui.utils

import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BasicAuthCredentials
import io.ktor.client.plugins.auth.providers.basic
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.Url
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import org.apache.solr.ui.domain.AuthOption

/**
 * Function that returns a simple HTTP client that is preconfigured with a base
 * URL.
 */
fun getDefaultClient(
    url: Url = Url("http://127.0.0.1:8983/"),
    block: HttpClientConfig<*>.() -> Unit = {},
) = HttpClient {
    defaultRequest {
        url(url.toString())
    }

    install(ContentNegotiation) {
        json(
            Json {
                ignoreUnknownKeys = true
                allowSpecialFloatingPointValues = true
            },
        )
    }

    block()
}

fun getHttpClientWithAuthOption(option: AuthOption) = when (option) {
    is AuthOption.None -> getDefaultClient(option.url)
    is AuthOption.BasicAuthOption -> getHttpClientWithCredentials(
        url = option.url,
        username = option.username,
        password = option.password,
    )
}

fun getHttpClientWithCredentials(
    url: Url = Url("http://127.0.0.1:8983/"),
    username: String,
    password: String,
) = getDefaultClient(url) {
    install(Auth) {
        basic {
            credentials { BasicAuthCredentials(username, password) }
        }
    }
}
