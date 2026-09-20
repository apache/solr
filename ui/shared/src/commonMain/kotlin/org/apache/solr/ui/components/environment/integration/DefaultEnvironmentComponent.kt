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

package org.apache.solr.ui.components.environment.integration

import io.ktor.client.HttpClient
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.environment.data.HttpEnvironmentRepository
import org.apache.solr.ui.components.environment.domain.DefaultLoadJavaPropertiesUseCase
import org.apache.solr.ui.components.environment.domain.DefaultLoadSystemDataUseCase
import org.apache.solr.ui.components.environment.domain.LoadJavaPropertiesUseCase
import org.apache.solr.ui.components.environment.domain.LoadSystemDataUseCase
import org.apache.solr.ui.components.environment.repository.EnvironmentRepository
import org.apache.solr.ui.components.environment.viewmodel.EnvironmentViewModel
import org.apache.solr.ui.utils.AppDispatchers
import org.apache.solr.ui.utils.platformDispatchers

/**
 * Default implementation of the [EnvironmentComponent].
 *
 * This implementation is using HTTP for environment operations.
 *
 * @param httpClient The pre-configured HTTP client to use for environment operations.
 */
class DefaultEnvironmentComponent(
    httpClient: HttpClient,
    private val dispatchers: AppDispatchers = platformDispatchers(),
) : EnvironmentComponent {

    override val environmentRepository: EnvironmentRepository by lazy {
        HttpEnvironmentRepository(httpClient)
    }

    override val loadSystemDataUseCase: LoadSystemDataUseCase by lazy {
        DefaultLoadSystemDataUseCase(environmentRepository)
    }

    override val loadJavaPropertiesUseCase: LoadJavaPropertiesUseCase by lazy {
        DefaultLoadJavaPropertiesUseCase(environmentRepository)
    }

    override fun createEnvironmentViewModel(): EnvironmentViewModel = EnvironmentViewModel(
        loadSystemDataUseCase = loadSystemDataUseCase,
        loadJavaPropertiesUseCase = loadJavaPropertiesUseCase,
        dispatchers = dispatchers,
    )
}
