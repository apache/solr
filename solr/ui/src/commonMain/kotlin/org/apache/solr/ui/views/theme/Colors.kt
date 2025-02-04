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

import androidx.compose.material3.ColorScheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.graphics.Color

/*
 * This file holds color values, classes and functions that are used for customizing
 * the color scheme of the material theme to match he Solr theme.
 *
 * Additional colors are also provided as ExtendedColorScheme. This makes it possible
 * to use custom color palettes aside the default color scheme that is available. This
 * is especially useful when we want to use colors for states of nodes or highlight
 * critical errors or warnings.
 */

val primaryLight = Color(0xFF9C1F00)
val onPrimaryLight = Color(0xFFFFFFFF)
val primaryContainerLight = Color(0xFFD53E1B)
val onPrimaryContainerLight = Color(0xFFFFFFFF)
val secondaryLight = Color(0xFF9B442F)
val onSecondaryLight = Color(0xFFFFFFFF)
val secondaryContainerLight = Color(0xFFFF9E87)
val onSecondaryContainerLight = Color(0xFF561103)
val tertiaryLight = Color(0xFF674C00)
val onTertiaryLight = Color(0xFFFFFFFF)
val tertiaryContainerLight = Color(0xFF946F00)
val onTertiaryContainerLight = Color(0xFFFFFFFF)
val errorLight = Color(0xFFBA1A1A)
val onErrorLight = Color(0xFFFFFFFF)
val errorContainerLight = Color(0xFFFFDAD6)
val onErrorContainerLight = Color(0xFF410002)
val backgroundLight = Color(0xFFFFF8F6)
val onBackgroundLight = Color(0xFF271814)
val surfaceLight = Color(0xFFFCF8F8)
val onSurfaceLight = Color(0xFF1C1B1B)
val surfaceVariantLight = Color(0xFFE0E3E3)
val onSurfaceVariantLight = Color(0xFF444748)
val outlineLight = Color(0xFF747878)
val outlineVariantLight = Color(0xFFC4C7C7)
val scrimLight = Color(0xFF000000)
val inverseSurfaceLight = Color(0xFF313030)
val inverseOnSurfaceLight = Color(0xFFF4F0EF)
val inversePrimaryLight = Color(0xFFFFB4A3)
val surfaceDimLight = Color(0xFFDDD9D9)
val surfaceBrightLight = Color(0xFFFCF8F8)
val surfaceContainerLowestLight = Color(0xFFFFFFFF)
val surfaceContainerLowLight = Color(0xFFF7F3F2)
val surfaceContainerLight = Color(0xFFF1EDEC)
val surfaceContainerHighLight = Color(0xFFEBE7E7)
val surfaceContainerHighestLight = Color(0xFFE5E2E1)

val primaryLightMediumContrast = Color(0xFF841800)
val onPrimaryLightMediumContrast = Color(0xFFFFFFFF)
val primaryContainerLightMediumContrast = Color(0xFFD53E1B)
val onPrimaryContainerLightMediumContrast = Color(0xFFFFFFFF)
val secondaryLightMediumContrast = Color(0xFF772917)
val onSecondaryLightMediumContrast = Color(0xFFFFFFFF)
val secondaryContainerLightMediumContrast = Color(0xFFB75943)
val onSecondaryContainerLightMediumContrast = Color(0xFFFFFFFF)
val tertiaryLightMediumContrast = Color(0xFF563F00)
val onTertiaryLightMediumContrast = Color(0xFFFFFFFF)
val tertiaryContainerLightMediumContrast = Color(0xFF946F00)
val onTertiaryContainerLightMediumContrast = Color(0xFFFFFFFF)
val errorLightMediumContrast = Color(0xFF8C0009)
val onErrorLightMediumContrast = Color(0xFFFFFFFF)
val errorContainerLightMediumContrast = Color(0xFFDA342E)
val onErrorContainerLightMediumContrast = Color(0xFFFFFFFF)
val backgroundLightMediumContrast = Color(0xFFFFF8F6)
val onBackgroundLightMediumContrast = Color(0xFF271814)
val surfaceLightMediumContrast = Color(0xFFFCF8F8)
val onSurfaceLightMediumContrast = Color(0xFF1C1B1B)
val surfaceVariantLightMediumContrast = Color(0xFFE0E3E3)
val onSurfaceVariantLightMediumContrast = Color(0xFF404344)
val outlineLightMediumContrast = Color(0xFF5C6060)
val outlineVariantLightMediumContrast = Color(0xFF787B7C)
val scrimLightMediumContrast = Color(0xFF000000)
val inverseSurfaceLightMediumContrast = Color(0xFF313030)
val inverseOnSurfaceLightMediumContrast = Color(0xFFF4F0EF)
val inversePrimaryLightMediumContrast = Color(0xFFFFB4A3)
val surfaceDimLightMediumContrast = Color(0xFFDDD9D9)
val surfaceBrightLightMediumContrast = Color(0xFFFCF8F8)
val surfaceContainerLowestLightMediumContrast = Color(0xFFFFFFFF)
val surfaceContainerLowLightMediumContrast = Color(0xFFF7F3F2)
val surfaceContainerLightMediumContrast = Color(0xFFF1EDEC)
val surfaceContainerHighLightMediumContrast = Color(0xFFEBE7E7)
val surfaceContainerHighestLightMediumContrast = Color(0xFFE5E2E1)

val primaryLightHighContrast = Color(0xFF490900)
val onPrimaryLightHighContrast = Color(0xFFFFFFFF)
val primaryContainerLightHighContrast = Color(0xFF841800)
val onPrimaryContainerLightHighContrast = Color(0xFFFFFFFF)
val secondaryLightHighContrast = Color(0xFF490900)
val onSecondaryLightHighContrast = Color(0xFFFFFFFF)
val secondaryContainerLightHighContrast = Color(0xFF772917)
val onSecondaryContainerLightHighContrast = Color(0xFFFFFFFF)
val tertiaryLightHighContrast = Color(0xFF2E2000)
val onTertiaryLightHighContrast = Color(0xFFFFFFFF)
val tertiaryContainerLightHighContrast = Color(0xFF563F00)
val onTertiaryContainerLightHighContrast = Color(0xFFFFFFFF)
val errorLightHighContrast = Color(0xFF4E0002)
val onErrorLightHighContrast = Color(0xFFFFFFFF)
val errorContainerLightHighContrast = Color(0xFF8C0009)
val onErrorContainerLightHighContrast = Color(0xFFFFFFFF)
val backgroundLightHighContrast = Color(0xFFFFF8F6)
val onBackgroundLightHighContrast = Color(0xFF271814)
val surfaceLightHighContrast = Color(0xFFFCF8F8)
val onSurfaceLightHighContrast = Color(0xFF000000)
val surfaceVariantLightHighContrast = Color(0xFFE0E3E3)
val onSurfaceVariantLightHighContrast = Color(0xFF212525)
val outlineLightHighContrast = Color(0xFF404344)
val outlineVariantLightHighContrast = Color(0xFF404344)
val scrimLightHighContrast = Color(0xFF000000)
val inverseSurfaceLightHighContrast = Color(0xFF313030)
val inverseOnSurfaceLightHighContrast = Color(0xFFFFFFFF)
val inversePrimaryLightHighContrast = Color(0xFFFFE7E2)
val surfaceDimLightHighContrast = Color(0xFFDDD9D9)
val surfaceBrightLightHighContrast = Color(0xFFFCF8F8)
val surfaceContainerLowestLightHighContrast = Color(0xFFFFFFFF)
val surfaceContainerLowLightHighContrast = Color(0xFFF7F3F2)
val surfaceContainerLightHighContrast = Color(0xFFF1EDEC)
val surfaceContainerHighLightHighContrast = Color(0xFFEBE7E7)
val surfaceContainerHighestLightHighContrast = Color(0xFFE5E2E1)

val primaryDark = Color(0xFFFFB4A3)
val onPrimaryDark = Color(0xFF631000)
val primaryContainerDark = Color(0xFFCD3916)
val onPrimaryContainerDark = Color(0xFFFFFFFF)
val secondaryDark = Color(0xFFFFB4A3)
val onSecondaryDark = Color(0xFF5E1707)
val secondaryContainerDark = Color(0xFF732615)
val onSecondaryContainerDark = Color(0xFFFFC9BD)
val tertiaryDark = Color(0xFFEFC054)
val onTertiaryDark = Color(0xFF3F2E00)
val tertiaryContainerDark = Color(0xFF8D6900)
val onTertiaryContainerDark = Color(0xFFFFFFFF)
val errorDark = Color(0xFFFFB4AB)
val onErrorDark = Color(0xFF690005)
val errorContainerDark = Color(0xFF93000A)
val onErrorContainerDark = Color(0xFFFFDAD6)
val backgroundDark = Color(0xFF1E100D)
val onBackgroundDark = Color(0xFFF9DCD6)
val surfaceDark = Color(0xFF141313)
val onSurfaceDark = Color(0xFFE5E2E1)
val surfaceVariantDark = Color(0xFF444748)
val onSurfaceVariantDark = Color(0xFFC4C7C7)
val outlineDark = Color(0xFF8E9192)
val outlineVariantDark = Color(0xFF444748)
val scrimDark = Color(0xFF000000)
val inverseSurfaceDark = Color(0xFFE5E2E1)
val inverseOnSurfaceDark = Color(0xFF313030)
val inversePrimaryDark = Color(0xFFB52703)
val surfaceDimDark = Color(0xFF141313)
val surfaceBrightDark = Color(0xFF3A3939)
val surfaceContainerLowestDark = Color(0xFF0E0E0E)
val surfaceContainerLowDark = Color(0xFF1C1B1B)
val surfaceContainerDark = Color(0xFF201F1F)
val surfaceContainerHighDark = Color(0xFF2A2A2A)
val surfaceContainerHighestDark = Color(0xFF353434)

val primaryDarkMediumContrast = Color(0xFFFFBAAA)
val onPrimaryDarkMediumContrast = Color(0xFF330500)
val primaryContainerDarkMediumContrast = Color(0xFFFC5934)
val onPrimaryContainerDarkMediumContrast = Color(0xFF000000)
val secondaryDarkMediumContrast = Color(0xFFFFBAAA)
val onSecondaryDarkMediumContrast = Color(0xFF330500)
val secondaryContainerDarkMediumContrast = Color(0xFFDA745C)
val onSecondaryContainerDarkMediumContrast = Color(0xFF000000)
val tertiaryDarkMediumContrast = Color(0xFFF3C458)
val onTertiaryDarkMediumContrast = Color(0xFF1F1500)
val tertiaryContainerDarkMediumContrast = Color(0xFFB48B22)
val onTertiaryContainerDarkMediumContrast = Color(0xFF000000)
val errorDarkMediumContrast = Color(0xFFFFBAB1)
val onErrorDarkMediumContrast = Color(0xFF370001)
val errorContainerDarkMediumContrast = Color(0xFFFF5449)
val onErrorContainerDarkMediumContrast = Color(0xFF000000)
val backgroundDarkMediumContrast = Color(0xFF1E100D)
val onBackgroundDarkMediumContrast = Color(0xFFF9DCD6)
val surfaceDarkMediumContrast = Color(0xFF141313)
val onSurfaceDarkMediumContrast = Color(0xFFFEFAF9)
val surfaceVariantDarkMediumContrast = Color(0xFF444748)
val onSurfaceVariantDarkMediumContrast = Color(0xFFC8CBCC)
val outlineDarkMediumContrast = Color(0xFFA0A3A4)
val outlineVariantDarkMediumContrast = Color(0xFF808484)
val scrimDarkMediumContrast = Color(0xFF000000)
val inverseSurfaceDarkMediumContrast = Color(0xFFE5E2E1)
val inverseOnSurfaceDarkMediumContrast = Color(0xFF2A2A2A)
val inversePrimaryDarkMediumContrast = Color(0xFF8D1B00)
val surfaceDimDarkMediumContrast = Color(0xFF141313)
val surfaceBrightDarkMediumContrast = Color(0xFF3A3939)
val surfaceContainerLowestDarkMediumContrast = Color(0xFF0E0E0E)
val surfaceContainerLowDarkMediumContrast = Color(0xFF1C1B1B)
val surfaceContainerDarkMediumContrast = Color(0xFF201F1F)
val surfaceContainerHighDarkMediumContrast = Color(0xFF2A2A2A)
val surfaceContainerHighestDarkMediumContrast = Color(0xFF353434)

val primaryDarkHighContrast = Color(0xFFFFF9F8)
val onPrimaryDarkHighContrast = Color(0xFF000000)
val primaryContainerDarkHighContrast = Color(0xFFFFBAAA)
val onPrimaryContainerDarkHighContrast = Color(0xFF000000)
val secondaryDarkHighContrast = Color(0xFFFFF9F8)
val onSecondaryDarkHighContrast = Color(0xFF000000)
val secondaryContainerDarkHighContrast = Color(0xFFFFBAAA)
val onSecondaryContainerDarkHighContrast = Color(0xFF000000)
val tertiaryDarkHighContrast = Color(0xFFFFFAF7)
val onTertiaryDarkHighContrast = Color(0xFF000000)
val tertiaryContainerDarkHighContrast = Color(0xFFF3C458)
val onTertiaryContainerDarkHighContrast = Color(0xFF000000)
val errorDarkHighContrast = Color(0xFFFFF9F9)
val onErrorDarkHighContrast = Color(0xFF000000)
val errorContainerDarkHighContrast = Color(0xFFFFBAB1)
val onErrorContainerDarkHighContrast = Color(0xFF000000)
val backgroundDarkHighContrast = Color(0xFF1E100D)
val onBackgroundDarkHighContrast = Color(0xFFF9DCD6)
val surfaceDarkHighContrast = Color(0xFF141313)
val onSurfaceDarkHighContrast = Color(0xFFFFFFFF)
val surfaceVariantDarkHighContrast = Color(0xFF444748)
val onSurfaceVariantDarkHighContrast = Color(0xFFF9FBFC)
val outlineDarkHighContrast = Color(0xFFC8CBCC)
val outlineVariantDarkHighContrast = Color(0xFFC8CBCC)
val scrimDarkHighContrast = Color(0xFF000000)
val inverseSurfaceDarkHighContrast = Color(0xFFE5E2E1)
val inverseOnSurfaceDarkHighContrast = Color(0xFF000000)
val inversePrimaryDarkHighContrast = Color(0xFF570D00)
val surfaceDimDarkHighContrast = Color(0xFF141313)
val surfaceBrightDarkHighContrast = Color(0xFF3A3939)
val surfaceContainerLowestDarkHighContrast = Color(0xFF0E0E0E)
val surfaceContainerLowDarkHighContrast = Color(0xFF1C1B1B)
val surfaceContainerDarkHighContrast = Color(0xFF201F1F)
val surfaceContainerHighDarkHighContrast = Color(0xFF2A2A2A)
val surfaceContainerHighestDarkHighContrast = Color(0xFF353434)

val warningLight = Color(0xFF745B00)
val onWarningLight = Color(0xFFFFFFFF)
val warningContainerLight = Color(0xFFFFD450)
val onWarningContainerLight = Color(0xFF524000)
val healthyLight = Color(0xFF176D23)
val onHealthyLight = Color(0xFFFFFFFF)
val healthyContainerLight = Color(0xFF64B663)
val onHealthyContainerLight = Color(0xFF001F03)
val replicaLight = Color(0xFF005474)
val onReplicaLight = Color(0xFFFFFFFF)
val replicaContainerLight = Color(0xFF007BA7)
val onReplicaContainerLight = Color(0xFFFFFFFF)
val recoveryLight = Color(0xFF5F6300)
val onRecoveryLight = Color(0xFFFFFFFF)
val recoveryContainerLight = Color(0xFFDBE311)
val onRecoveryContainerLight = Color(0xFF434600)

val warningLightMediumContrast = Color(0xFF534100)
val onWarningLightMediumContrast = Color(0xFFFFFFFF)
val warningContainerLightMediumContrast = Color(0xFF8F7100)
val onWarningContainerLightMediumContrast = Color(0xFFFFFFFF)
val healthyLightMediumContrast = Color(0xFF004F12)
val onHealthyLightMediumContrast = Color(0xFFFFFFFF)
val healthyContainerLightMediumContrast = Color(0xFF338437)
val onHealthyContainerLightMediumContrast = Color(0xFFFFFFFF)
val replicaLightMediumContrast = Color(0xFF004864)
val onReplicaLightMediumContrast = Color(0xFFFFFFFF)
val replicaContainerLightMediumContrast = Color(0xFF007BA7)
val onReplicaContainerLightMediumContrast = Color(0xFFFFFFFF)
val recoveryLightMediumContrast = Color(0xFF434600)
val onRecoveryLightMediumContrast = Color(0xFFFFFFFF)
val recoveryContainerLightMediumContrast = Color(0xFF757A00)
val onRecoveryContainerLightMediumContrast = Color(0xFFFFFFFF)

val warningLightHighContrast = Color(0xFF2C2100)
val onWarningLightHighContrast = Color(0xFFFFFFFF)
val warningContainerLightHighContrast = Color(0xFF534100)
val onWarningContainerLightHighContrast = Color(0xFFFFFFFF)
val healthyLightHighContrast = Color(0xFF002906)
val onHealthyLightHighContrast = Color(0xFFFFFFFF)
val healthyContainerLightHighContrast = Color(0xFF004F12)
val onHealthyContainerLightHighContrast = Color(0xFFFFFFFF)
val replicaLightHighContrast = Color(0xFF002536)
val onReplicaLightHighContrast = Color(0xFFFFFFFF)
val replicaContainerLightHighContrast = Color(0xFF004864)
val onReplicaContainerLightHighContrast = Color(0xFFFFFFFF)
val recoveryLightHighContrast = Color(0xFF222400)
val onRecoveryLightHighContrast = Color(0xFFFFFFFF)
val recoveryContainerLightHighContrast = Color(0xFF434600)
val onRecoveryContainerLightHighContrast = Color(0xFFFFFFFF)

val warningDark = Color(0xFFFFF8EF)
val onWarningDark = Color(0xFF3D2F00)
val warningContainerDark = Color(0xFFFAC800)
val onWarningContainerDark = Color(0xFF4A3900)
val healthyDark = Color(0xFF85DA82)
val onHealthyDark = Color(0xFF00390A)
val healthyContainerDark = Color(0xFF338437)
val onHealthyContainerDark = Color(0xFFFFFFFF)
val replicaDark = Color(0xFF7CD0FF)
val onReplicaDark = Color(0xFF00344A)
val replicaContainerDark = Color(0xFF007199)
val onReplicaContainerDark = Color(0xFFFFFFFF)
val recoveryDark = Color(0xFFFDFFB7)
val onRecoveryDark = Color(0xFF313300)
val recoveryContainerDark = Color(0xFFD1D800)
val onRecoveryContainerDark = Color(0xFF3D3F00)

val warningDarkMediumContrast = Color(0xFFFFF8EF)
val onWarningDarkMediumContrast = Color(0xFF3D2F00)
val warningContainerDarkMediumContrast = Color(0xFFFAC800)
val onWarningContainerDarkMediumContrast = Color(0xFF221900)
val healthyDarkMediumContrast = Color(0xFF8ADE85)
val onHealthyDarkMediumContrast = Color(0xFF001B03)
val healthyContainerDarkMediumContrast = Color(0xFF50A251)
val onHealthyContainerDarkMediumContrast = Color(0xFF000000)
val replicaDarkMediumContrast = Color(0xFF89D4FF)
val onReplicaDarkMediumContrast = Color(0xFF001925)
val replicaContainerDarkMediumContrast = Color(0xFF3C9AC7)
val onReplicaContainerDarkMediumContrast = Color(0xFF000000)
val recoveryDarkMediumContrast = Color(0xFFFDFFB7)
val onRecoveryDarkMediumContrast = Color(0xFF313300)
val recoveryContainerDarkMediumContrast = Color(0xFFD1D800)
val onRecoveryContainerDarkMediumContrast = Color(0xFF1B1D00)

val warningDarkHighContrast = Color(0xFFFFFAF6)
val onWarningDarkHighContrast = Color(0xFF000000)
val warningContainerDarkHighContrast = Color(0xFFFAC800)
val onWarningContainerDarkHighContrast = Color(0xFF000000)
val healthyDarkHighContrast = Color(0xFFF1FFEA)
val onHealthyDarkHighContrast = Color(0xFF000000)
val healthyContainerDarkHighContrast = Color(0xFF8ADE85)
val onHealthyContainerDarkHighContrast = Color(0xFF000000)
val replicaDarkHighContrast = Color(0xFFF8FBFF)
val onReplicaDarkHighContrast = Color(0xFF000000)
val replicaContainerDarkHighContrast = Color(0xFF89D4FF)
val onReplicaContainerDarkHighContrast = Color(0xFF000000)
val recoveryDarkHighContrast = Color(0xFFFDFFBC)
val onRecoveryDarkHighContrast = Color(0xFF000000)
val recoveryContainerDarkHighContrast = Color(0xFFD1D800)
val onRecoveryContainerDarkHighContrast = Color(0xFF000000)

@Composable
fun extendedColorScheme(useDarkTheme: Boolean) = when {
    useDarkTheme -> extendedDark
    else -> extendedLight
}

/**
 * Data class that holds additional colors to extend the default [ColorScheme] colors.
 */
@Immutable
data class ExtendedColorScheme(
    val warning: ColorFamily,
    val healthy: ColorFamily,
    val replica: ColorFamily,
    val recovery: ColorFamily,
)

/**
 * Providable composition local for retrieving the current [ExtendedTypography] values.
 */
internal val LocalExtendedColorScheme = staticCompositionLocalOf { extendedLight }

internal val lightScheme = lightColorScheme(
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

internal val darkScheme = darkColorScheme(
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

internal val mediumContrastLightColorScheme = lightColorScheme(
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

internal val highContrastLightColorScheme = lightColorScheme(
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

internal val mediumContrastDarkColorScheme = darkColorScheme(
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

internal val highContrastDarkColorScheme = darkColorScheme(
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

internal val extendedLight = ExtendedColorScheme(
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

internal val extendedDark = ExtendedColorScheme(
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

internal val extendedLightMediumContrast = ExtendedColorScheme(
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

internal val extendedLightHighContrast = ExtendedColorScheme(
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

internal val extendedDarkMediumContrast = ExtendedColorScheme(
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

internal val extendedDarkHighContrast = ExtendedColorScheme(
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
