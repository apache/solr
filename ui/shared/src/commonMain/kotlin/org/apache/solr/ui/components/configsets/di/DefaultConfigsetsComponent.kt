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

package org.apache.solr.ui.components.configsets.di

import io.ktor.client.HttpClient
import org.apache.solr.ui.components.configsets.data.HttpConfigsetsRepository
import org.apache.solr.ui.components.configsets.domain.DefaultLoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsRouteViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsViewModel
import org.apache.solr.ui.utils.AppDispatchers
import org.apache.solr.ui.utils.platformDispatchers

/**
 * Default implementation of [ConfigsetsComponent].
 *
 * This implementation is using HTTP for configsets operations.
 *
 * @param httpClient The pre-configured HTTP client to use for user registration operations.
 */
class DefaultConfigsetsComponent(
    httpClient: HttpClient,
    private val dispatchers: AppDispatchers = platformDispatchers(),
) : ConfigsetsComponent {

    override val configsetsRepository: ConfigsetsRepository by lazy {
        HttpConfigsetsRepository(httpClient)
    }

    override val loadConfigsetsUseCase: LoadConfigsetsUseCase by lazy {
        DefaultLoadConfigsetsUseCase(configsetsRepository)
    }

    override fun createConfigsetsRouteViewModel(): ConfigsetsRouteViewModel = ConfigsetsRouteViewModel()

    override fun createConfigsetsViewModel(): ConfigsetsViewModel = ConfigsetsViewModel(loadConfigsetsUseCase, dispatchers)

    override fun createConfigsetsOverviewComponent(): ConfigsetsOverviewComponent = DefaultConfigsetsOverviewComponent(configsetsRepository)
}
