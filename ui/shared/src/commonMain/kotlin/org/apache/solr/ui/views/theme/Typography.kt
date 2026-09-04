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

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.runtime.Composable
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.sp

/*
 * This file holds typography values, including custom fonts and text styles that are used
 * to customize the default typography provided by the Material theme.
 *
 * Additional typography styles are provided as ExtendedTypography. This extension is
 * used for example to provide text styles for code blocks.
 */

/**
 * Custom typography that styles headlines and titles with a different font.
 */
@Composable
fun SolrTypography(): Typography = Typography(
    headlineLarge = MaterialTheme.typography.headlineLarge.copy(
        fontWeight = FontWeight.Light,
        fontSize = 32.sp,
        lineHeight = 40.sp,
        letterSpacing = 0.sp,
    ),
    headlineMedium = MaterialTheme.typography.headlineMedium.copy(
        fontWeight = FontWeight.Light,
        fontSize = 28.sp,
        lineHeight = 36.sp,
        letterSpacing = 0.sp,
    ),
    headlineSmall = MaterialTheme.typography.headlineSmall.copy(
        fontWeight = FontWeight.Light,
        fontSize = 24.sp,
        lineHeight = 32.sp,
        letterSpacing = 0.sp,
    ),
)

// Customize the extended typography
@Composable
fun extendedTypography() = ExtendedTypography(
    codeSmall = SolrTheme.typography.codeSmall,
    codeMedium = SolrTheme.typography.codeMedium,
    codeLarge = SolrTheme.typography.codeLarge,
)

/**
 * Data class that holds additional text styles to extend the default [Typography] text style sets.
 */
@Immutable
data class ExtendedTypography(
    val codeSmall: TextStyle = TextStyle.Default.copy(
        fontFamily = FontFamily.Monospace,
        fontWeight = FontWeight.Medium,
        fontSize = 11.sp,
        lineHeight = 16.0.sp,
        letterSpacing = 0.5.sp,
    ),
    val codeMedium: TextStyle = TextStyle.Default.copy(
        fontFamily = FontFamily.Monospace,
        fontWeight = FontWeight.Medium,
        fontSize = 12.sp,
        lineHeight = 16.0.sp,
        letterSpacing = 0.5.sp,
    ),
    val codeLarge: TextStyle = TextStyle.Default.copy(
        fontFamily = FontFamily.Monospace,
        fontWeight = FontWeight.Medium,
        fontSize = 14.sp,
        lineHeight = 20.0.sp,
        letterSpacing = 0.1.sp,
    ),
)

/**
 * Providable composition local for retrieving the current [ExtendedTypography] values.
 */
internal val LocalExtendedTypography = staticCompositionLocalOf {
    ExtendedTypography(
        codeSmall = TextStyle.Default.copy(
            fontFamily = FontFamily.Monospace,
            fontWeight = FontWeight.Medium,
            fontSize = 11.sp,
            lineHeight = 16.0.sp,
            letterSpacing = 0.5.sp,
        ),
        codeMedium = TextStyle.Default.copy(
            fontFamily = FontFamily.Monospace,
            fontWeight = FontWeight.Medium,
            fontSize = 12.sp,
            lineHeight = 16.0.sp,
            letterSpacing = 0.5.sp,
        ),
        codeLarge = TextStyle.Default.copy(
            fontFamily = FontFamily.Monospace,
            fontWeight = FontWeight.Medium,
            fontSize = 14.sp,
            lineHeight = 20.0.sp,
            letterSpacing = 0.1.sp,
        ),
    )
}
