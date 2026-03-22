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
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsRouteViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsViewModel
import org.apache.solr.ui.components.configsets.viewmodel.CreateConfigsetViewModel
import org.apache.solr.ui.components.configsets.viewmodel.ImportConfigsetViewModel
import org.apache.solr.ui.components.files.domain.SelectFileUseCase

/**
 * The configsets component keeps record of the currently available configsets, and a selected
 * configset that may be used for additional operations.
 */
interface ConfigsetsComponent {

    /**
     * Dependencies provided by the application.
     */
    val configsetsRepository: ConfigsetsRepository

    /**
     * Use case responsible for creating a new configset.
     */
    val createConfigsetUseCase: CreateConfigsetUseCase

    /**
     * Use case responsible for importing a configset from a file.
     */
    val importConfigsetUseCase: ImportConfigsetUseCase

    /**
     * Use case responsible for loading the available configsets.
     */
    val loadConfigsetsUseCase: LoadConfigsetsUseCase

    /**
     * Use case responsible for selecting a configset file.
     */
    val selectFileUseCase: SelectFileUseCase

    /**
     * Factory method to create a [ConfigsetsRouteViewModel] instance.
     */
    fun createConfigsetsRouteViewModel(): ConfigsetsRouteViewModel

    /**
     * Factory method to create a [ConfigsetsViewModel] instance.
     */
    fun createConfigsetsViewModel(): ConfigsetsViewModel

    /**
     * Factory method to create a [CreateConfigsetViewModel] instance.
     */
    fun createCreateConfigsetViewModel(): CreateConfigsetViewModel

    /**
     * Factory method to create a [ImportConfigsetViewModel] instance.
     */
    fun createImportConfigsetViewModel(): ImportConfigsetViewModel

    /**
     * Factory method to create a [ConfigsetsOverviewViewModel] instance.
     */
    fun createConfigsetsOverviewViewModel(): ConfigsetsOverviewViewModel
}
