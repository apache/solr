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

package org.apache.solr.ui.components.start.integration

import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.http.URLParserException
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.store.StartStore
import org.apache.solr.ui.errors.HostNotFoundException
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.error_invalid_url
import org.apache.solr.ui.generated.resources.error_solr_host_not_found
import org.apache.solr.ui.generated.resources.error_unknown

internal val startStateToModel: (StartStore.State) -> StartComponent.Model = {
    StartComponent.Model(
        url = it.url,
        error = it.error?.let { error ->
            when (error) {
                is URLParserException -> Res.string.error_invalid_url
                is HostNotFoundException -> Res.string.error_solr_host_not_found
                is ConnectTimeoutException -> Res.string.error_solr_host_not_found
                else -> Res.string.error_unknown
            }
        },
    )
}
