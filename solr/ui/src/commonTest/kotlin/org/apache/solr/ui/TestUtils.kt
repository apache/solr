package org.apache.solr.ui

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockEngineConfig
import io.ktor.client.engine.mock.MockRequestHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope

/**
 * Mocks the HTTP client to simulate a Solr instance.
 */
fun mockHttpClient(
    cloudEnabled: Boolean = true,
    authEnabled: Boolean = false,
): HttpClient {
    TODO("Not yet implemented")
}


fun TestScope.createMockEngine(vararg handlers: MockRequestHandler): MockEngine {
    return MockEngine(
        MockEngineConfig().apply {
            dispatcher = StandardTestDispatcher(testScheduler)
            handlers.forEach(::addHandler)
        }
    )
}