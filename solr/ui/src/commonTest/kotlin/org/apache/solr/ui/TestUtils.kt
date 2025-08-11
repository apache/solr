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
): MockEngine = MockEngine(
    MockEngineConfig().apply {
        dispatcher = StandardTestDispatcher(testScheduler)
        handlers.forEach(::addHandler)
        this.reuseHandlers = reuseHandlers
    },
)
