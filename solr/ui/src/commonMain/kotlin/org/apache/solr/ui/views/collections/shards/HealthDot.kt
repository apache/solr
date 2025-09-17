package org.apache.solr.ui.views.collections.shards

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.layout.size
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

private val HealthGreen = Color(0xFF2E7D32) // md green 700
private val HealthYellow = Color(0xFFF9A825) // md amber 800
private val HealthRed = Color(0xFFC62828) // md red 800

@Composable
fun HealthDot(status: String?, modifier: Modifier = Modifier, size: Dp = 10.dp) {
    val s = status?.trim()?.lowercase()
    val color = when (s) {
        "green", "healthy", "good" -> HealthGreen
        "yellow", "degraded", "recovering" -> HealthYellow
        "red", "down", "failed" -> HealthRed
        else -> MaterialTheme.colorScheme.outlineVariant
    }
    Canvas(Modifier.size(size)) {
        val r = size.toPx() / 2f
        drawCircle(color, radius = r) // core
        drawCircle(Color(0x33000000), radius = r, style = Stroke(1.dp.toPx())) // subtle ring
    }
}
