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

package org.apache.solr.ui.views.environment

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.widthIn
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.environment.EnvironmentComponent

/**
 * Composable for loading the environment section.
 *
 * This composable checks the window size and rearranges the content to achieve a better
 * representation.
 *
 * @param component The component that holds the state of this composable and handles interactions.
 * @param modifier Modifier to apply to the root composable.
 */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun EnvironmentContent(
    component: EnvironmentComponent,
    modifier: Modifier = Modifier,
) = FlowRow(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val model by component.model.collectAsState()

    val minChildWidth: Dp = 400.dp
    val maxChildWidth: Dp = 800.dp

    VersionsCard(
        modifier = Modifier.weight(1f)
            .widthIn(min = minChildWidth, max = maxChildWidth),
        versions = model.versions,
        jvm = model.jvm,
    )
    CommandLineArgumentsCard(
        modifier = Modifier.weight(1f)
            .widthIn(min = minChildWidth, max = maxChildWidth),
        arguments = model.jvm.jmx.commandLineArgs,
    )
    JavaPropertiesCard(
        modifier = Modifier.weight(1f)
            .widthIn(min = minChildWidth, max = maxChildWidth),
        properties = model.javaProperties,
    )
}
