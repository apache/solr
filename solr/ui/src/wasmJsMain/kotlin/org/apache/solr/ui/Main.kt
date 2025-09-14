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

package org.apache.solr.ui

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Surface
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.window.ComposeViewport
import com.arkivanov.decompose.DefaultComponentContext
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import io.ktor.http.Url
import kotlinx.browser.document
import kotlinx.browser.window
import kotlinx.coroutines.Dispatchers
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.integration.SimpleRootComponent
import org.apache.solr.ui.utils.DefaultAppComponentContext
import org.apache.solr.ui.utils.getDefaultClient
import org.apache.solr.ui.views.root.RootContent
import org.apache.solr.ui.views.theme.SolrTheme

/**
 * Entry point of the Compose application for all wasmJs (browser) targets.
 *
 * This function sets up the lifecycle management and instantiates the [RootComponent] with the
 * [DefaultAppComponentContext] that is used by all child components.
 */
@OptIn(ExperimentalComposeUiApi::class)
fun main() {
    val lifecycle = LifecycleRegistry()
    val componentContext = DefaultAppComponentContext(
        componentContext = DefaultComponentContext(lifecycle = lifecycle),
        mainContext = Dispatchers.Main,
        ioContext = Dispatchers.Default,
    )

    val url = Url(window.location.href)
    val destination = url.parameters["dest"]

    // TODO Set default request url to values from window location
    val httpClient = getDefaultClient()

    val component: RootComponent = SimpleRootComponent(
        componentContext = componentContext,
        storeFactory = DefaultStoreFactory(),
        httpClient = httpClient,
        destination = destination,
    )

    ComposeViewport(document.body!!) {
        SolrTheme(useDarkTheme = isSystemInDarkTheme()) {
            Surface(modifier = Modifier.fillMaxSize()) {
                RootContent(component)
            }
        }
    }
}
