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

package org.apache.solr.ui.components.auth.integration

import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import org.apache.solr.ui.components.auth.UnauthenticatedComponent
import org.apache.solr.ui.utils.AppComponentContext

class DefaultUnauthenticatedComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    private val output: (UnauthenticatedComponent.Output) -> Unit
) : UnauthenticatedComponent, AppComponentContext by componentContext {

    // TODO Implement me

    override fun onAbort() = output(UnauthenticatedComponent.Output.OnAbort)
}