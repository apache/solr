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

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.surfaceColorAtElevation
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.theme.SolrTheme

/**
 * A card that displays the provided command line arguments in a list.
 *
 * @param modifier Modifier to apply to the root composable.
 * @param arguments Command line arguments to display.
 */
@Composable
internal fun CommandLineArgumentsCard(
    arguments: List<String>,
    modifier: Modifier = Modifier,
) = SolrCard(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    Text(
        text = "Command Line Arguments",
        style = MaterialTheme.typography.headlineSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Column(
        modifier = Modifier.fillMaxWidth()
            .border(BorderStroke(1.dp, MaterialTheme.colorScheme.outlineVariant)),
    ) {
        arguments.forEachIndexed { index, argument ->
            Text(
                modifier = Modifier.fillMaxWidth()
                    .background(
                        MaterialTheme.colorScheme.surfaceColorAtElevation(
                            if (index % 2 == 0) 1.dp else 0.dp,
                        ),
                    ).padding(horizontal = 8.dp, vertical = 4.dp),
                text = argument,
                style = SolrTheme.typography.codeLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}
