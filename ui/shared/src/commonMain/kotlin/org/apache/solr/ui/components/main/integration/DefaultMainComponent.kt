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

package org.apache.solr.ui.components.main.integration

import io.ktor.client.HttpClient
import org.apache.solr.ui.components.cluster.ClusterComponent
import org.apache.solr.ui.components.cluster.integration.DefaultClusterComponent
import org.apache.solr.ui.components.configsets.di.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.di.DefaultConfigsetsComponent
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.environment.integration.DefaultEnvironmentComponent
import org.apache.solr.ui.components.logging.LoggingComponent
import org.apache.solr.ui.components.logging.integration.DefaultLoggingComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.viewmodel.MainViewModel

/**
 * Default implementation of the [MainComponent].
 *
 * @param httpClient The pre-configured HTTP client to use for the sections' operations.
 * @param destination The section to show initially, if any.
 */
class DefaultMainComponent(
    private val httpClient: HttpClient,
    private val destination: String? = null,
) : MainComponent {

    override val clusterComponent: ClusterComponent by lazy { DefaultClusterComponent() }

    override val configsetsComponent: ConfigsetsComponent by lazy {
        DefaultConfigsetsComponent(httpClient = httpClient)
    }

    override val environmentComponent: EnvironmentComponent by lazy {
        DefaultEnvironmentComponent(httpClient = httpClient)
    }

    override val loggingComponent: LoggingComponent by lazy { DefaultLoggingComponent() }

    override fun createMainViewModel(): MainViewModel = MainViewModel(destination = destination)
}
