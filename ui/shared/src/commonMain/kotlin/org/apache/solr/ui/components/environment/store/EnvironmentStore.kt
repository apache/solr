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

package org.apache.solr.ui.components.environment.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.JvmData
import org.apache.solr.ui.components.environment.data.SecurityConfig
import org.apache.solr.ui.components.environment.data.SystemInformation
import org.apache.solr.ui.components.environment.data.SystemMode
import org.apache.solr.ui.components.environment.data.Versions
import org.apache.solr.ui.components.environment.store.EnvironmentStore.Intent
import org.apache.solr.ui.components.environment.store.EnvironmentStore.State

/**
 * State store interface of the environment.
 *
 * Implementations of this state store manage detailed information of the environment.
 */
internal interface EnvironmentStore : Store<Intent, State, Nothing> {

    /**
     * Intent for interacting with the environment store.
     */
    sealed interface Intent {

        /**
         * Intent for requesting system data.
         */
        data object FetchSystemData : Intent
    }

    /**
     * State class that holds the data of the [EnvironmentStore].
     */
    data class State(
        val mode: SystemMode = SystemMode.Unknown,
        val zkHost: String = "",
        val solrHome: String = "",
        val coreRoot: String = "",
        val lucene: Versions = Versions(),
        val jvm: JvmData = JvmData(),
        val security: SecurityConfig = SecurityConfig(),
        val system: SystemInformation = SystemInformation(),
        val node: String = "",
        val javaProperties: List<JavaProperty> = emptyList(),
    )
}
