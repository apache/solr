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

package org.apache.solr.ui.components.start.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.start.store.StartStore.Intent
import org.apache.solr.ui.components.start.store.StartStore.State

/**
 * State store interface of the environment.
 *
 * Implementations of this state store manage detailed information of the environment.
 */
internal interface StartStore : Store<Intent, State, Nothing> {

    /**
     * Intent for interacting with the environment store.
     */
    sealed interface Intent {

        /**
         * Intent for updating the current Solr URL.
         */
        data class UpdateSolrUrl(val url: String): Intent

        /**
         * Intent for initiating a new connection to a Solr instance.
         */
        data object Connect: Intent
    }

    /**
     * State class that holds the data of the [StartStore].
     */
    data class State(
        val url: String = "",
        // TODO Add connection state (like connecting, connected, failed, auth required)
    )
}
