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

package org.apache.solr.ui.preview.configsets

import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import kotlinx.coroutines.Dispatchers
import org.apache.solr.ui.components.configsets.di.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.di.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsRouteViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsViewModel
import org.apache.solr.ui.components.configsets.viewmodel.CreateConfigsetViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ImportConfigsetViewModel
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.configsets.ConfigsetsScene

@Preview
@Composable
private fun PreviewConfigsetsContentEmptyConfigsets() = PreviewContainer {
    ConfigsetsScene(component = PreviewConfigsetsComponent())
}

private class PreviewConfigsetsComponent(
    private val configsets: List<Configset> = emptyList(),
) : ConfigsetsComponent {

    override val configsetsRepository: ConfigsetsRepository = error("Not used in previews")

    override val loadConfigsetsUseCase: LoadConfigsetsUseCase = object : LoadConfigsetsUseCase {
        override suspend fun invoke(): Result<List<Configset>> = Result.success(configsets)
    }

    override fun createConfigsetsRouteViewModel(): ConfigsetsRouteViewModel =
        ConfigsetsRouteViewModel()

    override fun createConfigsetsViewModel(): ConfigsetsViewModel = ConfigsetsViewModel(
        loadConfigsetsUseCase = loadConfigsetsUseCase,
        ioDispatcher = Dispatchers.Default,
    )

    override fun createConfigsetsOverviewComponent(): ConfigsetsOverviewComponent =
        PreviewConfigsetsOverviewComponent(configsets)
}

private class PreviewConfigsetsOverviewComponent(configsets: List<Configset> = emptyList()) : ConfigsetsOverviewComponent {

    override val configsetsRepository: ConfigsetsRepository = error("Not used in previews")

    override val createConfigsetUseCase: CreateConfigsetUseCase = error("Not used in previews")

    override val importConfigsetUseCase: ImportConfigsetUseCase = error("Not used in previews")

    override val loadConfigsetsUseCase: LoadConfigsetsUseCase = object : LoadConfigsetsUseCase {
        override suspend fun invoke(): Result<List<Configset>> = Result.success(configsets)
    }

    override val selectFileUseCase: SelectFileUseCase = error("Not used in previews")

    override fun createConfigsetsViewModel(): ConfigsetsViewModel = ConfigsetsViewModel(
        loadConfigsetsUseCase = loadConfigsetsUseCase,
        ioDispatcher = Dispatchers.Default,
    )

    override fun createCreateConfigsetViewModel(): CreateConfigsetViewModel =
        CreateConfigsetViewModel(
            createConfigsetUseCase = createConfigsetUseCase,
            loadConfigsetsUseCase = loadConfigsetsUseCase,
            ioDispatcher = Dispatchers.Default
        )

    override fun createImportConfigsetViewModel(): ImportConfigsetViewModel =
        ImportConfigsetViewModel(
            importConfigsetUseCase = importConfigsetUseCase,
            selectFileUseCase = selectFileUseCase,
            ioDispatcher = Dispatchers.Default,
        )

    override fun createConfigsetsOverviewViewModel(): ConfigsetsOverviewViewModel =
        ConfigsetsOverviewViewModel()
}
