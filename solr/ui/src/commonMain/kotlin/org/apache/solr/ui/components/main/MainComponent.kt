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

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.value.Value
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.logging.LoggingComponent
import org.apache.solr.ui.components.navigation.NavigationComponent
import org.apache.solr.ui.views.navigation.MainMenu

/**
 * Main component of the application that is used as base for users with access.
 *
 * Note that this component can be accessed if the user is either authenticated or if the Solr
 * instance accessed does not have any authentication enabled.
 */
interface MainComponent : NavigationComponent {

    /**
     * Child stack that holds the navigation state.
     */
    val childStack: Value<ChildStack<*, Child>>

    /**
     * Handles navigation requests from a navigation menu.
     *
     * @param menuItem The destination to navigate to.
     */
    fun onNavigate(menuItem: MainMenu)

    /**
     * Handles logout requests.
     */
    fun onLogout()

    /**
     * Child interface that defines all available children of the [MainComponent].
     */
    sealed interface Child {

        // TODO Uncomment once DashboardComponent available
        // data class Dashboard(val component: DashboardComponent): Child

        // TODO Uncomment once MetricsComponent available
        // data class Metrics(val component: MetricsComponent): Child

        // TODO Uncomment once ClusterComponent available
        // data class Cluster(val component: ClusterComponent): Child

        // TODO Uncomment once SecurityComponent available
        // data class Security(val component: SecurityComponent): Child

        // TODO Uncomment once ConfigsetsComponent available
        // data class Configsets(val component: ConfigsetsComponent): Child

        // TODO Uncomment once MetricsComponent available
        // data class Collections(val component: CollectionsComponent): Child

        // TODO Uncomment once QueriesAndOperationsComponent available
        // data class QueriesAndOperations(val component: QueriesAndOperationsComponent): Child

        /**
         * Child that leads to the environment section.
         *
         * @property component Component that holds the state of this child.
         */
        data class Environment(val component: EnvironmentComponent) : Child

        /**
         * Child that leads to the logging section.
         *
         * @property component Component that holds the state of this child.
         */
        data class Logging(val component: LoggingComponent) : Child

        // TODO Uncomment once ThreadDump available
        // data class ThreadDump(val component: ThreadDumpComponent): Child
    }

    sealed interface Output {

        /**
         * Output that is returned when the user logs out.
         */
        data object UserLoggedOut : Output
    }
}
