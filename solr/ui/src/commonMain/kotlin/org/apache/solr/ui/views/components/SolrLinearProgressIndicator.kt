package org.apache.solr.ui.views.components

import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.ProgressIndicatorDefaults
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

/**
 * A [LinearProgressIndicator] which uses sharp edges instead of round as defaults.
 *
 * @see [LinearProgressIndicator]
 */
@Composable
fun SolrLinearProgressIndicator(
    modifier: Modifier = Modifier,
    color: Color = ProgressIndicatorDefaults.linearColor,
    trackColor: Color = ProgressIndicatorDefaults.linearTrackColor,
    strokeCap: StrokeCap = StrokeCap.Butt,
    gapSize: Dp = 0.dp,
) = LinearProgressIndicator(
    modifier = modifier,
    color = color,
    trackColor = trackColor,
    strokeCap = strokeCap,
    gapSize = gapSize,
)
