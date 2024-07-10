package org.apache.solr.composeui.ui.theme

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.runtime.Composable
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontStyle
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.sp
import org.apache.solr.compose_ui.generated.resources.Res
import org.apache.solr.compose_ui.generated.resources.raleway_variable
import org.jetbrains.compose.resources.Font

/**
 * Custom typography that styles headlines and titles with a different font.
 */
@Composable
fun SolrTypography() = Typography(
    headlineLarge = MaterialTheme.typography.headlineLarge.copy(
        fontFamily = FontFamily(Font(Res.font.raleway_variable)),
        fontWeight = FontWeight.Normal,
        fontSize = 32.sp,
        lineHeight = 40.sp,
        letterSpacing= 0.sp,
    ),
    headlineMedium = MaterialTheme.typography.headlineMedium.copy(
        fontFamily = FontFamily(Font(Res.font.raleway_variable)),
        fontWeight = FontWeight.Normal,
        fontSize = 28.sp,
        lineHeight = 36.sp,
        letterSpacing= 0.sp,
    ),
    headlineSmall = MaterialTheme.typography.headlineSmall.copy(
        fontFamily = FontFamily(Font(Res.font.raleway_variable)),
        fontWeight = FontWeight.Normal,
        fontSize = 24.sp,
        lineHeight = 32.sp,
        letterSpacing= 0.sp,
    )
)
