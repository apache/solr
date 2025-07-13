package org.apache.solr.ui

import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockEngineConfig
import io.ktor.client.engine.mock.MockRequestHandler
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope

/**
 * Creates a mock engine with the given handlers and a standard test dispatcher.
 *
 * @param handlers The handlers to attach to the mock engine that will handle the requests
 * in the same order.
 * 
 */
fun TestScope.createMockEngine(
    vararg handlers: MockRequestHandler,
    reuseHandlers: Boolean = true,
): MockEngine {
    return MockEngine(
        MockEngineConfig().apply {
            dispatcher = StandardTestDispatcher(testScheduler)
            handlers.forEach(::addHandler)
            this.reuseHandlers = reuseHandlers
        }
    )
}
