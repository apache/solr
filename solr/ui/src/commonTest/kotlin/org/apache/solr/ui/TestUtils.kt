package org.apache.solr.ui

import io.ktor.client.HttpClient
import kotlinx.coroutines.CoroutineScope

/**
 * Mocks the HTTP client to simulate a Solr instance.
 */
fun mockHttpClient(
    cloudEnabled: Boolean = true,
    authEnabled: Boolean = false,
): HttpClient {
    TODO("Not yet implemented")
}
