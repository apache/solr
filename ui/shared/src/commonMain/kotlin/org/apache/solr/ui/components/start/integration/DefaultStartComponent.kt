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

package org.apache.solr.ui.components.start.integration

import io.ktor.client.HttpClient
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.data.HttpStartRepository
import org.apache.solr.ui.components.start.domain.ConnectUseCase
import org.apache.solr.ui.components.start.domain.DefaultConnectUseCase
import org.apache.solr.ui.components.start.repository.StartRepository
import org.apache.solr.ui.components.start.viewmodel.StartViewModel
import org.apache.solr.ui.utils.AppDispatchers
import org.apache.solr.ui.utils.platformDispatchers

/**
 * Default implementation of the [StartComponent].
 *
 * This implementation is using HTTP for establishing connections.
 *
 * @param httpClient The pre-configured HTTP client to use for connection attempts.
 */
class DefaultStartComponent(
    httpClient: HttpClient,
    private val dispatchers: AppDispatchers = platformDispatchers(),
) : StartComponent {

    override val startRepository: StartRepository by lazy {
        HttpStartRepository(httpClient)
    }

    override val connectUseCase: ConnectUseCase by lazy {
        DefaultConnectUseCase(startRepository)
    }

    override fun createStartViewModel(): StartViewModel = StartViewModel(
        connectUseCase = connectUseCase,
        dispatchers = dispatchers,
    )
}
