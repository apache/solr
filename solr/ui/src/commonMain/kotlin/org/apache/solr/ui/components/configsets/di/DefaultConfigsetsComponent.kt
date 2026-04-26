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
import kotlinx.coroutines.Dispatchers
import org.apache.solr.ui.components.configsets.CreateConfigsetViewModel
import org.apache.solr.ui.components.configsets.ConfigsetsOverviewViewModel
import org.apache.solr.ui.components.configsets.ConfigsetsViewModel
import org.apache.solr.ui.components.configsets.data.HttpConfigsetsRepository
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultCreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultLoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository

/**
 * The configsets component keeps record of the currently available configsets, and a selected
 * configset that may be used for additional operations.
 */
class DefaultConfigsetsComponent(httpClient: HttpClient) : ConfigsetsComponent {

    private val ioDispatcher by lazy { Dispatchers.Unconfined } // TODO Change to platformDispatchers

    /**
     * Dependencies provided by the application.
     */
    override val configsetsRepository: ConfigsetsRepository by lazy {
        HttpConfigsetsRepository(httpClient)
    }

    /**
     * Use case responsible for creating a new configset.
     */
    override val createConfigsetUseCase: CreateConfigsetUseCase by lazy {
        DefaultCreateConfigsetUseCase(configsetsRepository)
    }

    /**
     * Use case responsible for importing a configset from a file.
     */
    override val importConfigsetUseCase: ImportConfigsetUseCase by lazy {
        DefaultImportConfigsetUseCase(configsetsRepository)
    }

    /**
     * Use case responsible for loading the available configsets.
     */
    override val loadConfigsetsUseCase: LoadConfigsetsUseCase by lazy {
        DefaultLoadConfigsetsUseCase(configsetsRepository)
    }

    /**
     * Factory method to create a [ConfigsetsViewModel] instance.
     */
    override fun createConfigsetsViewModel(): ConfigsetsViewModel =
        ConfigsetsViewModel(loadConfigsetsUseCase, ioDispatcher)

    /**
     * Factory method to create a [CreateConfigsetViewModel] instance.
     */
    override fun createCreateConfigsetViewModel(): CreateConfigsetViewModel =
        CreateConfigsetViewModel(createConfigsetUseCase, ioDispatcher)
    // TODO See how to populate configsets for selection here too

    /**
     * Factory method to create a [ConfigsetsOverviewViewModel] instance.
     */
    override fun createConfigsetsOverviewViewModel(): ConfigsetsOverviewViewModel =
        ConfigsetsOverviewViewModel()
}
