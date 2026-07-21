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

package org.apache.solr.ui.views.components

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

/**
 * A simplified card used to wrap content in a styled [Column].
 *
 * @param modifier Modifier to apply to the root component. Note that some attributes
 * are set by default.
 * @param verticalArrangement Vertical arrangement to apply to the column.
 * @param horizontalAlignment Horizontal alignment to apply to the column.
 * @param content The card's content.
 */
@Composable
fun SolrCard(
    modifier: Modifier = Modifier,
    verticalArrangement: Arrangement.Vertical = Arrangement.Top,
    horizontalAlignment: Alignment.Horizontal = Alignment.Start,
    content: @Composable ColumnScope.() -> Unit,
) = Column(
    modifier = modifier
        .background(MaterialTheme.colorScheme.surfaceContainer)
        .border(BorderStroke(1.dp, MaterialTheme.colorScheme.outlineVariant))
        .padding(16.dp),
    verticalArrangement = verticalArrangement,
    horizontalAlignment = horizontalAlignment,
    content = content,
)
