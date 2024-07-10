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

package org.apache.solr.composeui.ui.environment

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.windowsizeclass.ExperimentalMaterial3WindowSizeClassApi
import androidx.compose.material3.windowsizeclass.WindowWidthSizeClass
import androidx.compose.material3.windowsizeclass.calculateWindowSizeClass
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.composeui.components.environment.EnvironmentComponent

@OptIn(ExperimentalMaterial3WindowSizeClassApi::class)
@Composable
fun EnvironmentContent(
    component: EnvironmentComponent,
    modifier: Modifier = Modifier,
) {
    val model by component.model.collectAsState()
    val windowSizeClass = calculateWindowSizeClass()
    val isLargeScreen = windowSizeClass.widthSizeClass > WindowWidthSizeClass.Medium

    if (isLargeScreen) EnvironmentContentExpanded(
        modifier = modifier,
        model = model,
    ) else EnvironmentContentMedium(
        modifier = modifier,
        model = model,
    )
}

@Composable
private fun EnvironmentContentExpanded(
    model: EnvironmentComponent.Model,
    modifier: Modifier = Modifier,
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
) {
    Column(
        modifier = Modifier.weight(1f),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        VersionsCard(
            modifier = Modifier.fillMaxWidth(),
            versions = model.lucene,
            jvm = model.jvm,
        )
        JavaPropertiesCard(
            modifier = Modifier.fillMaxWidth(),
            properties = model.javaProperties,
        )
    }
    CommandLineArgumentsCard(
        modifier = Modifier.weight(1f),
        arguments = model.jvm.jmx.commandLineArgs,
    )
}

@Composable
private fun EnvironmentContentMedium(
    model: EnvironmentComponent.Model,
    modifier: Modifier = Modifier,
) = Column(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    VersionsCard(
        modifier = Modifier.fillMaxWidth(),
        versions = model.lucene,
        jvm = model.jvm,
    )
    JavaPropertiesCard(
        modifier = Modifier.fillMaxWidth(),
        properties = model.javaProperties,
    )
    CommandLineArgumentsCard(
        modifier = Modifier.fillMaxWidth(),
        arguments = model.jvm.jmx.commandLineArgs,
    )
}
