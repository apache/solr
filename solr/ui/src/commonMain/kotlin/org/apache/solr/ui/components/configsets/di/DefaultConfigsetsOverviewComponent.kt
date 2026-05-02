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

import org.apache.solr.ui.components.configsets.domain.CreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultCreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.DefaultLoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsViewModel
import org.apache.solr.ui.components.configsets.viewmodel.CreateConfigsetViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ImportConfigsetViewModel
import org.apache.solr.ui.components.files.domain.DefaultSelectFileUseCase
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.utils.AppDispatchers
import org.apache.solr.ui.utils.platformDispatchers

/**
 * Default implementation of [ConfigsetsOverviewComponent].
 */
class DefaultConfigsetsOverviewComponent(
    override val configsetsRepository: ConfigsetsRepository,
    private val dispatchers: AppDispatchers = platformDispatchers(),
) : ConfigsetsOverviewComponent {

    override val createConfigsetUseCase: CreateConfigsetUseCase by lazy {
        DefaultCreateConfigsetUseCase(configsetsRepository)
    }

    override val importConfigsetUseCase: ImportConfigsetUseCase by lazy {
        DefaultImportConfigsetUseCase(configsetsRepository)
    }

    // TODO Consider implementing special SelectFileUseCase for configsets import cases
    override val selectFileUseCase: SelectFileUseCase by lazy { DefaultSelectFileUseCase() }

    override val loadConfigsetsUseCase: LoadConfigsetsUseCase by lazy {
        DefaultLoadConfigsetsUseCase(configsetsRepository)
    }

    override fun createConfigsetsViewModel(): ConfigsetsViewModel = ConfigsetsViewModel(loadConfigsetsUseCase, dispatchers)

    override fun createCreateConfigsetViewModel(): CreateConfigsetViewModel = CreateConfigsetViewModel(createConfigsetUseCase, loadConfigsetsUseCase, dispatchers)

    override fun createImportConfigsetViewModel(): ImportConfigsetViewModel = ImportConfigsetViewModel(importConfigsetUseCase, selectFileUseCase, dispatchers)

    override fun createConfigsetsOverviewViewModel(): ConfigsetsOverviewViewModel = ConfigsetsOverviewViewModel()
}
