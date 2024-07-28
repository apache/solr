package org.apache.solr.composeui.ui.theme

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
fun SolrTypography(): Typography {
    val ralewayFontFamily = Fonts.raleway()

    return Typography(
        headlineLarge = MaterialTheme.typography.headlineLarge.copy(
            fontFamily = ralewayFontFamily,
            fontWeight = FontWeight.Normal,
            fontSize = 32.sp,
            lineHeight = 40.sp,
            letterSpacing= 0.sp,
        ),
        headlineMedium = MaterialTheme.typography.headlineMedium.copy(
            fontFamily = ralewayFontFamily,
            fontWeight = FontWeight.Normal,
            fontSize = 28.sp,
            lineHeight = 36.sp,
            letterSpacing= 0.sp,
        ),
        headlineSmall = MaterialTheme.typography.headlineSmall.copy(
            fontFamily = ralewayFontFamily,
            fontWeight = FontWeight.Normal,
            fontSize = 24.sp,
            lineHeight = 32.sp,
            letterSpacing= 0.sp,
        )
    )
}

// Customize the extended typography
@Composable
fun extendedTypography() = ExtendedTypography(
    codeSmall = SolrTheme.typography.codeSmall.copy(fontFamily = Fonts.firacode()),
    codeMedium = SolrTheme.typography.codeMedium.copy(fontFamily = Fonts.firacode()),
    codeLarge = SolrTheme.typography.codeLarge.copy(fontFamily = Fonts.firacode()),
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
