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

package org.apache.solr.composeui

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.window.ComposeViewport
import com.arkivanov.decompose.DefaultComponentContext
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import kotlinx.browser.document
import kotlinx.coroutines.Dispatchers
import org.apache.solr.composeui.components.root.RootComponent
import org.apache.solr.composeui.components.root.integration.DefaultRootComponent
import org.apache.solr.composeui.ui.root.RootContent
import org.apache.solr.composeui.ui.theme.SolrTheme
import org.apache.solr.composeui.utils.DefaultAppComponentContext

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

    val component: RootComponent = DefaultRootComponent(
        componentContext = componentContext,
        storeFactory = DefaultStoreFactory(),
    )

    ComposeViewport(document.body!!) {
        SolrTheme(useDarkTheme = isSystemInDarkTheme()) {
            Surface(modifier = Modifier.fillMaxSize()) {
                RootContent(component)
            }
        }
    }
}
