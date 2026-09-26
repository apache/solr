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

package org.apache.solr.ui.components.main

import org.apache.solr.ui.components.cluster.ClusterComponent
import org.apache.solr.ui.components.configsets.di.ConfigsetsComponent
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.logging.LoggingComponent
import org.apache.solr.ui.components.main.viewmodel.MainViewModel

/**
 * Main component of the application that is used as base for users with access.
 *
 * Note that this component can be accessed if the user is either authenticated or if the Solr
 * instance accessed does not have any authentication enabled.
 */
interface MainComponent {

    // TODO Add DashboardComponent once available

    // TODO Add MetricsComponent once available

    /**
     * Component of the cluster section.
     */
    val clusterComponent: ClusterComponent

    // TODO Add SecurityComponent once available

    /**
     * Component of the configsets section.
     */
    val configsetsComponent: ConfigsetsComponent

    // TODO Add CollectionsComponent once available

    // TODO Add QueriesAndOperationsComponent once available

    /**
     * Component of the environment section.
     */
    val environmentComponent: EnvironmentComponent

    /**
     * Component of the logging section.
     */
    val loggingComponent: LoggingComponent

    // TODO Add ThreadDumpComponent once available

    /**
     * Factory method to create a [MainViewModel] instance.
     */
    fun createMainViewModel(): MainViewModel
}
