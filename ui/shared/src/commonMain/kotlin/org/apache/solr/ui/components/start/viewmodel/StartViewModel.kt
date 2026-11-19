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

package org.apache.solr.ui.components.start.viewmodel

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import org.apache.solr.ui.components.start.domain.ConnectUseCase
import org.apache.solr.ui.utils.AppDispatchers

class StartViewModel(
    connectUseCase: ConnectUseCase,
    dispatchers: AppDispatchers,
) : ViewModel() {

    private val startState = StartStateHolder(
        scope = viewModelScope,
        connectUseCase = connectUseCase,
        dispatchers = dispatchers,
    )

    /**
     * UI state of the start screen.
     */
    val uiState = startState.uiState

    /**
     * Events emitted by the start screen.
     */
    val events = startState.events

    /**
     * Updates the Solr URL.
     *
     * @param url The new Solr URL value.
     */
    fun changeSolrUrl(url: String) = startState.changeSolrUrl(url)

    /**
     * Connects to the Solr instance with the current URL.
     */
    fun connect() = startState.connect()
}
