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

package org.apache.solr.ui.components.environment.viewmodel

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.JvmData
import org.apache.solr.ui.components.environment.data.SecurityConfig
import org.apache.solr.ui.components.environment.data.SystemInformation
import org.apache.solr.ui.components.environment.data.SystemMode
import org.apache.solr.ui.components.environment.data.Versions
import org.apache.solr.ui.components.environment.domain.LoadJavaPropertiesUseCase
import org.apache.solr.ui.components.environment.domain.LoadSystemDataUseCase
import org.apache.solr.ui.utils.AppDispatchers

/**
 * State holder of the environment that manages detailed information of the environment.
 *
 * The environment data is fetched initially, and can be fetched again with [fetchSystemData].
 */
class EnvironmentStateHolder(
    private val scope: CoroutineScope,
    private val loadSystemDataUseCase: LoadSystemDataUseCase,
    private val loadJavaPropertiesUseCase: LoadJavaPropertiesUseCase,
    private val dispatchers: AppDispatchers,
) {

    /**
     * UI state of the environment.
     */
    val uiState: StateFlow<EnvironmentUiState>
        field = MutableStateFlow(EnvironmentUiState())

    init {
        fetchSystemData()
    }

    /**
     * Fetches the system data and the java properties that are part of the environment state.
     */
    fun fetchSystemData() {
        loadSystemData()
        loadJavaProperties()
    }

    /**
     * Fetches the system data that are part of the environment state.
     *
     * If successful, the state is updated with the new system data.
     */
    private fun loadSystemData() = scope.launch {
        // TODO Add coroutine exception handler
        withContext(dispatchers.io) {
            loadSystemDataUseCase()
        }.onSuccess { data ->
            uiState.update {
                it.copy(
                    mode = data.mode,
                    zkHost = data.zkHost,
                    solrHome = data.solrHome,
                    coreRoot = data.coreRoot,
                    lucene = data.lucene,
                    jvm = data.jvm,
                    security = data.security,
                    system = data.system,
                    node = data.node,
                )
            }
        }
        // TODO Add error handling
    }

    /**
     * Fetches the java properties that are part of the environment state.
     *
     * If successful, the state is updated with the new java properties.
     */
    private fun loadJavaProperties() = scope.launch {
        // TODO Add coroutine exception handler
        withContext(dispatchers.io) {
            loadJavaPropertiesUseCase()
        }.onSuccess { properties ->
            uiState.update { it.copy(javaProperties = properties) }
        }
        // TODO Add error handling
    }
}

data class EnvironmentUiState(
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
