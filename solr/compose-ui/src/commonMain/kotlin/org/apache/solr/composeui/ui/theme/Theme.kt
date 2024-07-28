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

package org.apache.solr.composeui.ui.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.Immutable
import org.apache.solr.compose_ui.generated.resources.Res
import org.apache.solr.compose_ui.generated.resources.raleway_variable
import org.jetbrains.compose.resources.Font

@Immutable
data class ExtendedColorScheme(
    val warning: ColorFamily,
    val healthy: ColorFamily,
    val replica: ColorFamily,
    val recovery: ColorFamily,
)

private val lightScheme = lightColorScheme(
    primary = primaryLight,
    onPrimary = onPrimaryLight,
    primaryContainer = primaryContainerLight,
    onPrimaryContainer = onPrimaryContainerLight,
    secondary = secondaryLight,
    onSecondary = onSecondaryLight,
    secondaryContainer = secondaryContainerLight,
    onSecondaryContainer = onSecondaryContainerLight,
    tertiary = tertiaryLight,
    onTertiary = onTertiaryLight,
    tertiaryContainer = tertiaryContainerLight,
    onTertiaryContainer = onTertiaryContainerLight,
    error = errorLight,
    onError = onErrorLight,
    errorContainer = errorContainerLight,
    onErrorContainer = onErrorContainerLight,
    background = backgroundLight,
    onBackground = onBackgroundLight,
    surface = surfaceLight,
    onSurface = onSurfaceLight,
    surfaceVariant = surfaceVariantLight,
    onSurfaceVariant = onSurfaceVariantLight,
    outline = outlineLight,
    outlineVariant = outlineVariantLight,
    scrim = scrimLight,
    inverseSurface = inverseSurfaceLight,
    inverseOnSurface = inverseOnSurfaceLight,
    inversePrimary = inversePrimaryLight,
    surfaceDim = surfaceDimLight,
    surfaceBright = surfaceBrightLight,
    surfaceContainerLowest = surfaceContainerLowestLight,
    surfaceContainerLow = surfaceContainerLowLight,
    surfaceContainer = surfaceContainerLight,
    surfaceContainerHigh = surfaceContainerHighLight,
    surfaceContainerHighest = surfaceContainerHighestLight,
)

private val darkScheme = darkColorScheme(
    primary = primaryDark,
    onPrimary = onPrimaryDark,
    primaryContainer = primaryContainerDark,
    onPrimaryContainer = onPrimaryContainerDark,
    secondary = secondaryDark,
    onSecondary = onSecondaryDark,
    secondaryContainer = secondaryContainerDark,
    onSecondaryContainer = onSecondaryContainerDark,
    tertiary = tertiaryDark,
    onTertiary = onTertiaryDark,
    tertiaryContainer = tertiaryContainerDark,
    onTertiaryContainer = onTertiaryContainerDark,
    error = errorDark,
    onError = onErrorDark,
    errorContainer = errorContainerDark,
    onErrorContainer = onErrorContainerDark,
    background = backgroundDark,
    onBackground = onBackgroundDark,
    surface = surfaceDark,
    onSurface = onSurfaceDark,
    surfaceVariant = surfaceVariantDark,
    onSurfaceVariant = onSurfaceVariantDark,
    outline = outlineDark,
    outlineVariant = outlineVariantDark,
    scrim = scrimDark,
    inverseSurface = inverseSurfaceDark,
    inverseOnSurface = inverseOnSurfaceDark,
    inversePrimary = inversePrimaryDark,
    surfaceDim = surfaceDimDark,
    surfaceBright = surfaceBrightDark,
    surfaceContainerLowest = surfaceContainerLowestDark,
    surfaceContainerLow = surfaceContainerLowDark,
    surfaceContainer = surfaceContainerDark,
    surfaceContainerHigh = surfaceContainerHighDark,
    surfaceContainerHighest = surfaceContainerHighestDark,
)

private val mediumContrastLightColorScheme = lightColorScheme(
    primary = primaryLightMediumContrast,
    onPrimary = onPrimaryLightMediumContrast,
    primaryContainer = primaryContainerLightMediumContrast,
    onPrimaryContainer = onPrimaryContainerLightMediumContrast,
    secondary = secondaryLightMediumContrast,
    onSecondary = onSecondaryLightMediumContrast,
    secondaryContainer = secondaryContainerLightMediumContrast,
    onSecondaryContainer = onSecondaryContainerLightMediumContrast,
    tertiary = tertiaryLightMediumContrast,
    onTertiary = onTertiaryLightMediumContrast,
    tertiaryContainer = tertiaryContainerLightMediumContrast,
    onTertiaryContainer = onTertiaryContainerLightMediumContrast,
    error = errorLightMediumContrast,
    onError = onErrorLightMediumContrast,
    errorContainer = errorContainerLightMediumContrast,
    onErrorContainer = onErrorContainerLightMediumContrast,
    background = backgroundLightMediumContrast,
    onBackground = onBackgroundLightMediumContrast,
    surface = surfaceLightMediumContrast,
    onSurface = onSurfaceLightMediumContrast,
    surfaceVariant = surfaceVariantLightMediumContrast,
    onSurfaceVariant = onSurfaceVariantLightMediumContrast,
    outline = outlineLightMediumContrast,
    outlineVariant = outlineVariantLightMediumContrast,
    scrim = scrimLightMediumContrast,
    inverseSurface = inverseSurfaceLightMediumContrast,
    inverseOnSurface = inverseOnSurfaceLightMediumContrast,
    inversePrimary = inversePrimaryLightMediumContrast,
    surfaceDim = surfaceDimLightMediumContrast,
    surfaceBright = surfaceBrightLightMediumContrast,
    surfaceContainerLowest = surfaceContainerLowestLightMediumContrast,
    surfaceContainerLow = surfaceContainerLowLightMediumContrast,
    surfaceContainer = surfaceContainerLightMediumContrast,
    surfaceContainerHigh = surfaceContainerHighLightMediumContrast,
    surfaceContainerHighest = surfaceContainerHighestLightMediumContrast,
)

private val highContrastLightColorScheme = lightColorScheme(
    primary = primaryLightHighContrast,
    onPrimary = onPrimaryLightHighContrast,
    primaryContainer = primaryContainerLightHighContrast,
    onPrimaryContainer = onPrimaryContainerLightHighContrast,
    secondary = secondaryLightHighContrast,
    onSecondary = onSecondaryLightHighContrast,
    secondaryContainer = secondaryContainerLightHighContrast,
    onSecondaryContainer = onSecondaryContainerLightHighContrast,
    tertiary = tertiaryLightHighContrast,
    onTertiary = onTertiaryLightHighContrast,
    tertiaryContainer = tertiaryContainerLightHighContrast,
    onTertiaryContainer = onTertiaryContainerLightHighContrast,
    error = errorLightHighContrast,
    onError = onErrorLightHighContrast,
    errorContainer = errorContainerLightHighContrast,
    onErrorContainer = onErrorContainerLightHighContrast,
    background = backgroundLightHighContrast,
    onBackground = onBackgroundLightHighContrast,
    surface = surfaceLightHighContrast,
    onSurface = onSurfaceLightHighContrast,
    surfaceVariant = surfaceVariantLightHighContrast,
    onSurfaceVariant = onSurfaceVariantLightHighContrast,
    outline = outlineLightHighContrast,
    outlineVariant = outlineVariantLightHighContrast,
    scrim = scrimLightHighContrast,
    inverseSurface = inverseSurfaceLightHighContrast,
    inverseOnSurface = inverseOnSurfaceLightHighContrast,
    inversePrimary = inversePrimaryLightHighContrast,
    surfaceDim = surfaceDimLightHighContrast,
    surfaceBright = surfaceBrightLightHighContrast,
    surfaceContainerLowest = surfaceContainerLowestLightHighContrast,
    surfaceContainerLow = surfaceContainerLowLightHighContrast,
    surfaceContainer = surfaceContainerLightHighContrast,
    surfaceContainerHigh = surfaceContainerHighLightHighContrast,
    surfaceContainerHighest = surfaceContainerHighestLightHighContrast,
)

private val mediumContrastDarkColorScheme = darkColorScheme(
    primary = primaryDarkMediumContrast,
    onPrimary = onPrimaryDarkMediumContrast,
    primaryContainer = primaryContainerDarkMediumContrast,
    onPrimaryContainer = onPrimaryContainerDarkMediumContrast,
    secondary = secondaryDarkMediumContrast,
    onSecondary = onSecondaryDarkMediumContrast,
    secondaryContainer = secondaryContainerDarkMediumContrast,
    onSecondaryContainer = onSecondaryContainerDarkMediumContrast,
    tertiary = tertiaryDarkMediumContrast,
    onTertiary = onTertiaryDarkMediumContrast,
    tertiaryContainer = tertiaryContainerDarkMediumContrast,
    onTertiaryContainer = onTertiaryContainerDarkMediumContrast,
    error = errorDarkMediumContrast,
    onError = onErrorDarkMediumContrast,
    errorContainer = errorContainerDarkMediumContrast,
    onErrorContainer = onErrorContainerDarkMediumContrast,
    background = backgroundDarkMediumContrast,
    onBackground = onBackgroundDarkMediumContrast,
    surface = surfaceDarkMediumContrast,
    onSurface = onSurfaceDarkMediumContrast,
    surfaceVariant = surfaceVariantDarkMediumContrast,
    onSurfaceVariant = onSurfaceVariantDarkMediumContrast,
    outline = outlineDarkMediumContrast,
    outlineVariant = outlineVariantDarkMediumContrast,
    scrim = scrimDarkMediumContrast,
    inverseSurface = inverseSurfaceDarkMediumContrast,
    inverseOnSurface = inverseOnSurfaceDarkMediumContrast,
    inversePrimary = inversePrimaryDarkMediumContrast,
    surfaceDim = surfaceDimDarkMediumContrast,
    surfaceBright = surfaceBrightDarkMediumContrast,
    surfaceContainerLowest = surfaceContainerLowestDarkMediumContrast,
    surfaceContainerLow = surfaceContainerLowDarkMediumContrast,
    surfaceContainer = surfaceContainerDarkMediumContrast,
    surfaceContainerHigh = surfaceContainerHighDarkMediumContrast,
    surfaceContainerHighest = surfaceContainerHighestDarkMediumContrast,
)

private val highContrastDarkColorScheme = darkColorScheme(
    primary = primaryDarkHighContrast,
    onPrimary = onPrimaryDarkHighContrast,
    primaryContainer = primaryContainerDarkHighContrast,
    onPrimaryContainer = onPrimaryContainerDarkHighContrast,
    secondary = secondaryDarkHighContrast,
    onSecondary = onSecondaryDarkHighContrast,
    secondaryContainer = secondaryContainerDarkHighContrast,
    onSecondaryContainer = onSecondaryContainerDarkHighContrast,
    tertiary = tertiaryDarkHighContrast,
    onTertiary = onTertiaryDarkHighContrast,
    tertiaryContainer = tertiaryContainerDarkHighContrast,
    onTertiaryContainer = onTertiaryContainerDarkHighContrast,
    error = errorDarkHighContrast,
    onError = onErrorDarkHighContrast,
    errorContainer = errorContainerDarkHighContrast,
    onErrorContainer = onErrorContainerDarkHighContrast,
    background = backgroundDarkHighContrast,
    onBackground = onBackgroundDarkHighContrast,
    surface = surfaceDarkHighContrast,
    onSurface = onSurfaceDarkHighContrast,
    surfaceVariant = surfaceVariantDarkHighContrast,
    onSurfaceVariant = onSurfaceVariantDarkHighContrast,
    outline = outlineDarkHighContrast,
    outlineVariant = outlineVariantDarkHighContrast,
    scrim = scrimDarkHighContrast,
    inverseSurface = inverseSurfaceDarkHighContrast,
    inverseOnSurface = inverseOnSurfaceDarkHighContrast,
    inversePrimary = inversePrimaryDarkHighContrast,
    surfaceDim = surfaceDimDarkHighContrast,
    surfaceBright = surfaceBrightDarkHighContrast,
    surfaceContainerLowest = surfaceContainerLowestDarkHighContrast,
    surfaceContainerLow = surfaceContainerLowDarkHighContrast,
    surfaceContainer = surfaceContainerDarkHighContrast,
    surfaceContainerHigh = surfaceContainerHighDarkHighContrast,
    surfaceContainerHighest = surfaceContainerHighestDarkHighContrast,
)

val extendedLight = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningLight,
        onColor = onWarningLight,
        colorContainer = warningContainerLight,
        onColorContainer = onWarningContainerLight,
    ),
    healthy = ColorFamily(
        color = healthyLight,
        onColor = onHealthyLight,
        colorContainer = healthyContainerLight,
        onColorContainer = onHealthyContainerLight,
    ),
    replica = ColorFamily(
        color = replicaLight,
        onColor = onReplicaLight,
        colorContainer = replicaContainerLight,
        onColorContainer = onReplicaContainerLight,
    ),
    recovery = ColorFamily(
        color = recoveryLight,
        onColor = onRecoveryLight,
        colorContainer = recoveryContainerLight,
        onColorContainer = onRecoveryContainerLight,
    ),
)

val extendedDark = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningDark,
        onColor = onWarningDark,
        colorContainer = warningContainerDark,
        onColorContainer = onWarningContainerDark,
    ),
    healthy = ColorFamily(
        color = healthyDark,
        onColor = onHealthyDark,
        colorContainer = healthyContainerDark,
        onColorContainer = onHealthyContainerDark,
    ),
    replica = ColorFamily(
        color = replicaDark,
        onColor = onReplicaDark,
        colorContainer = replicaContainerDark,
        onColorContainer = onReplicaContainerDark,
    ),
    recovery = ColorFamily(
        color = recoveryDark,
        onColor = onRecoveryDark,
        colorContainer = recoveryContainerDark,
        onColorContainer = onRecoveryContainerDark,
    ),
)

val extendedLightMediumContrast = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningLightMediumContrast,
        onColor = onWarningLightMediumContrast,
        colorContainer = warningContainerLightMediumContrast,
        onColorContainer = onWarningContainerLightMediumContrast,
    ),
    healthy = ColorFamily(
        color = healthyLightMediumContrast,
        onColor = onHealthyLightMediumContrast,
        colorContainer = healthyContainerLightMediumContrast,
        onColorContainer = onHealthyContainerLightMediumContrast,
    ),
    replica = ColorFamily(
        color = replicaLightMediumContrast,
        onColor = onReplicaLightMediumContrast,
        colorContainer = replicaContainerLightMediumContrast,
        onColorContainer = onReplicaContainerLightMediumContrast,
    ),
    recovery = ColorFamily(
        color = recoveryLightMediumContrast,
        onColor = onRecoveryLightMediumContrast,
        colorContainer = recoveryContainerLightMediumContrast,
        onColorContainer = onRecoveryContainerLightMediumContrast,
    ),
)

val extendedLightHighContrast = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningLightHighContrast,
        onColor = onWarningLightHighContrast,
        colorContainer = warningContainerLightHighContrast,
        onColorContainer = onWarningContainerLightHighContrast,
    ),
    healthy = ColorFamily(
        color = healthyLightHighContrast,
        onColor = onHealthyLightHighContrast,
        colorContainer = healthyContainerLightHighContrast,
        onColorContainer = onHealthyContainerLightHighContrast,
    ),
    replica = ColorFamily(
        color = replicaLightHighContrast,
        onColor = onReplicaLightHighContrast,
        colorContainer = replicaContainerLightHighContrast,
        onColorContainer = onReplicaContainerLightHighContrast,
    ),
    recovery = ColorFamily(
        color = recoveryLightHighContrast,
        onColor = onRecoveryLightHighContrast,
        colorContainer = recoveryContainerLightHighContrast,
        onColorContainer = onRecoveryContainerLightHighContrast,
    ),
)

val extendedDarkMediumContrast = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningDarkMediumContrast,
        onColor = onWarningDarkMediumContrast,
        colorContainer = warningContainerDarkMediumContrast,
        onColorContainer = onWarningContainerDarkMediumContrast,
    ),
    healthy = ColorFamily(
        color = healthyDarkMediumContrast,
        onColor = onHealthyDarkMediumContrast,
        colorContainer = healthyContainerDarkMediumContrast,
        onColorContainer = onHealthyContainerDarkMediumContrast,
    ),
    replica = ColorFamily(
        color = replicaDarkMediumContrast,
        onColor = onReplicaDarkMediumContrast,
        colorContainer = replicaContainerDarkMediumContrast,
        onColorContainer = onReplicaContainerDarkMediumContrast,
    ),
    recovery = ColorFamily(
        color = recoveryDarkMediumContrast,
        onColor = onRecoveryDarkMediumContrast,
        colorContainer = recoveryContainerDarkMediumContrast,
        onColorContainer = onRecoveryContainerDarkMediumContrast,
    ),
)

val extendedDarkHighContrast = ExtendedColorScheme(
    warning = ColorFamily(
        color = warningDarkHighContrast,
        onColor = onWarningDarkHighContrast,
        colorContainer = warningContainerDarkHighContrast,
        onColorContainer = onWarningContainerDarkHighContrast,
    ),
    healthy = ColorFamily(
        color = healthyDarkHighContrast,
        onColor = onHealthyDarkHighContrast,
        colorContainer = healthyContainerDarkHighContrast,
        onColorContainer = onHealthyContainerDarkHighContrast,
    ),
    replica = ColorFamily(
        color = replicaDarkHighContrast,
        onColor = onReplicaDarkHighContrast,
        colorContainer = replicaContainerDarkHighContrast,
        onColorContainer = onReplicaContainerDarkHighContrast,
    ),
    recovery = ColorFamily(
        color = recoveryDarkHighContrast,
        onColor = onRecoveryDarkHighContrast,
        colorContainer = recoveryContainerDarkHighContrast,
        onColorContainer = onRecoveryContainerDarkHighContrast,
    ),
)

/**
 * Solr theme object that holds additional fields, like extended typography and extended colors.
 */
object SolrTheme {

    /**
     * Additional text styles found in [ExtendedTypography].
     */
    val typography: ExtendedTypography
        @Composable
        get() = LocalExtendedTypography.current
}

@Composable
fun SolrTheme(
    useDarkTheme: Boolean = isSystemInDarkTheme(),
    content: @Composable () -> Unit
) {
    val colorScheme = when {
        useDarkTheme -> darkScheme
        else -> lightScheme
    }

    // Customize the extended typography
    val extendedTypography = ExtendedTypography(
        codeSmall = SolrTheme.typography.codeSmall.copy(fontFamily = Fonts.firacode()),
        codeMedium = SolrTheme.typography.codeMedium.copy(fontFamily = Fonts.firacode()),
        codeLarge = SolrTheme.typography.codeLarge.copy(fontFamily = Fonts.firacode()),
    )
    Font(Res.font.raleway_variable)

    CompositionLocalProvider(LocalExtendedTypography provides extendedTypography) {
        MaterialTheme(
            colorScheme = colorScheme,
            shapes = SolrShapes,
            typography = SolrTypography(),
            content = content,
        )
    }
}
