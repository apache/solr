package org.apache.solr.composeui.utils

import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json

/**
 * Function that returns a simple HTTP client that is preconfigured with a base
 * URL.
 */
fun getDefaultClient() = HttpClient {
    defaultRequest {
        url("http://localhost:8983/")
    }
    install(ContentNegotiation) {
        json(
            Json {
                ignoreUnknownKeys = true
            }
        )
    }
}