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

package org.apache.solr.ui.views.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.InternalComposeUiApi
import androidx.compose.ui.LocalSystemTheme
import androidx.compose.ui.SystemTheme

/**
 * Solr theme object that holds additional fields, like extended typography and extended colors.
 */
object SolrTheme {

    /**
     * Additional text styles provided [ExtendedTypography].
     */
    val typography: ExtendedTypography
        @Composable
        get() = LocalExtendedTypography.current

    /**
     * Additional colors provided by [ExtendedColorScheme].
     */
    val colorScheme: ExtendedColorScheme
        @Composable
        get() = LocalExtendedColorScheme.current
}

@OptIn(InternalComposeUiApi::class)
@Composable
fun SolrTheme(
    useDarkTheme: Boolean = isSystemInDarkTheme(),
    content: @Composable () -> Unit,
) {
    val colorScheme = when {
        useDarkTheme -> darkScheme
        else -> lightScheme
    }

    CompositionLocalProvider(
        LocalExtendedTypography provides extendedTypography(),
        LocalExtendedColorScheme provides extendedColorScheme(useDarkTheme),
        LocalSystemTheme provides if (useDarkTheme) SystemTheme.Dark else SystemTheme.Light,
    ) {
        MaterialTheme(
            colorScheme = colorScheme,
            shapes = SolrShapes,
            typography = SolrTypography(),
            content = content,
        )
    }
}
