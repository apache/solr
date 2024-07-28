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

package org.apache.solr.composeui.ui.icons

import androidx.compose.foundation.layout.Box
import androidx.compose.material.icons.materialPath
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.graphics.vector.rememberVectorPainter
import androidx.compose.ui.unit.dp
import org.apache.solr.compose_ui.generated.resources.Res
import org.apache.solr.compose_ui.generated.resources.cd_solr_logo
import org.jetbrains.compose.resources.stringResource

/**
 * Colorized Solr logo.
 *
 * @param modifier Modifier that is applied to the wrapper of the logo.
 */
@Composable
fun SolrLogo(
    modifier: Modifier = Modifier,
) {
    val solrPainter = rememberVectorPainter(
        ImageVector.Builder(
            name = "Logos.SolrLogo",
            defaultWidth = 128.dp,
            defaultHeight = 64.dp,
            viewportWidth = 128f,
            viewportHeight = 64f,
            autoMirror = false,
        ).apply {
            materialPath {
                moveTo(25.7013f, 44.178f)
                curveTo(24.2211f, 43.4114f, 22.5512f, 42.8706f, 20.7359f, 42.5702f)
                curveTo(18.9503f, 42.277f, 17.1375f, 42.1305f, 15.342f, 42.1305f)
                curveTo(13.874f, 42.1305f, 12.3913f, 42.0055f, 10.9283f, 41.7628f)
                curveTo(9.50715f, 41.5248f, 8.21901f, 41.0899f, 7.09835f, 40.4674f)
                curveTo(6.00724f, 39.8618f, 5.10578f, 39.0159f, 4.41615f, 37.9489f)
                curveTo(3.73636f, 36.8987f, 3.394f, 35.5072f, 3.394f, 33.8274f)
                curveTo(3.42602f, 32.3446f, 3.77823f, 31.0782f, 4.44324f, 30.0568f)
                curveTo(5.1181f, 29.021f, 5.99739f, 28.1799f, 7.05894f, 27.5551f)
                curveTo(8.14758f, 26.9158f, 9.40371f, 26.4448f, 10.7928f, 26.1564f)
                curveTo(13.1573f, 25.6662f, 15.6548f, 25.5748f, 18.1991f, 25.9209f)
                curveTo(19.1941f, 26.0579f, 20.1744f, 26.3006f, 21.1202f, 26.6395f)
                curveTo(22.0463f, 26.9759f, 22.9182f, 27.4349f, 23.7088f, 28.0045f)
                curveTo(24.4871f, 28.5668f, 25.1767f, 29.2734f, 25.7555f, 30.1025f)
                lineTo(26.0782f, 30.5663f)
                lineTo(28.4673f, 29.5858f)
                lineTo(27.9599f, 28.8888f)
                curveTo(27.3097f, 27.9925f, 26.5905f, 27.185f, 25.8196f, 26.4905f)
                curveTo(25.0339f, 25.7815f, 24.1102f, 25.1783f, 23.0733f, 24.7025f)
                curveTo(22.0487f, 24.2315f, 20.8788f, 23.8686f, 19.5906f, 23.6234f)
                curveTo(18.3173f, 23.3831f, 16.8321f, 23.2606f, 15.177f, 23.2606f)
                curveTo(13.5883f, 23.2606f, 11.948f, 23.4384f, 10.3027f, 23.7893f)
                curveTo(8.63279f, 24.1449f, 7.08357f, 24.7361f, 5.69937f, 25.5412f)
                curveTo(4.28561f, 26.3655f, 3.12061f, 27.4637f, 2.23394f, 28.8071f)
                curveTo(1.33741f, 30.1721f, 0.879288f, 31.8568f, 0.879288f, 33.813f)
                curveTo(0.879288f, 35.8677f, 1.30292f, 37.6172f, 2.13542f, 39.0111f)
                curveTo(2.96544f, 40.3977f, 4.07625f, 41.52f, 5.43829f, 42.3467f)
                curveTo(6.77323f, 43.159f, 8.32984f, 43.743f, 10.0663f, 44.0818f)
                curveTo(11.7608f, 44.4135f, 13.5366f, 44.5817f, 15.342f, 44.5817f)
                curveTo(16.7779f, 44.5817f, 18.2976f, 44.6875f, 19.8591f, 44.8989f)
                curveTo(21.3788f, 45.1032f, 22.795f, 45.5166f, 24.0634f, 46.127f)
                curveTo(25.2999f, 46.723f, 26.3319f, 47.5521f, 27.1274f, 48.5902f)
                curveTo(27.8959f, 49.5972f, 28.285f, 50.955f, 28.285f, 52.6276f)
                curveTo(28.285f, 54.1296f, 27.9082f, 55.4105f, 27.1693f, 56.4391f)
                curveTo(26.4058f, 57.4989f, 25.4132f, 58.3737f, 24.2186f, 59.0393f)
                curveTo(22.9994f, 59.7194f, 21.6202f, 60.2217f, 20.1153f, 60.5269f)
                curveTo(18.5857f, 60.8393f, 17.0735f, 60.9979f, 15.6178f, 60.9979f)
                curveTo(13.177f, 60.9979f, 10.7633f, 60.5437f, 8.4456f, 59.6449f)
                curveTo(6.14024f, 58.751f, 4.0935f, 57.41f, 2.36694f, 55.658f)
                lineTo(1.90389f, 55.187f)
                lineTo(0f, 56.7972f)
                lineTo(0.534469f, 57.3186f)
                curveTo(2.23886f, 58.9817f, 4.35457f, 60.4308f, 6.82495f, 61.6252f)
                curveTo(9.32243f, 62.8364f, 12.2829f, 63.4492f, 15.6178f, 63.4492f)
                curveTo(17.204f, 63.4492f, 18.8887f, 63.2738f, 20.6226f, 62.9229f)
                curveTo(22.3812f, 62.5696f, 24.0339f, 61.964f, 25.5289f, 61.1205f)
                curveTo(27.0486f, 60.265f, 28.3121f, 59.1475f, 29.2875f, 57.7945f)
                curveTo(30.2899f, 56.4054f, 30.7973f, 54.6679f, 30.7973f, 52.6276f)
                curveTo(30.7973f, 50.5609f, 30.3219f, 48.8089f, 29.381f, 47.4247f)
                curveTo(28.455f, 46.0573f, 27.2161f, 44.9662f, 25.7013f, 44.178f)
                close()

                moveTo(48.1663f, 61.2936f)
                curveTo(46.4422f, 61.2936f, 44.8684f, 60.9503f, 43.4842f, 60.271f)
                curveTo(42.0802f, 59.5845f, 40.8414f, 58.65f, 39.802f, 57.4915f)
                curveTo(38.7552f, 56.3283f, 37.9252f, 54.9672f, 37.3316f, 53.4463f)
                curveTo(36.7355f, 51.9184f, 36.3957f, 50.3165f, 36.3267f, 48.7146f)
                curveTo(36.3267f, 47.2319f, 36.6124f, 45.7183f, 37.169f, 44.2117f)
                curveTo(37.7281f, 42.71f, 38.5409f, 41.3417f, 39.5877f, 40.1427f)
                curveTo(40.6295f, 38.9485f, 41.898f, 37.9616f, 43.361f, 37.2036f)
                curveTo(44.7969f, 36.4575f, 46.4127f, 36.0808f, 48.1663f, 36.0808f)
                curveTo(49.814f, 36.0808f, 51.3707f, 36.4336f, 52.7918f, 37.1297f)
                curveTo(54.2351f, 37.8353f, 55.5011f, 38.7816f, 56.5626f, 39.9401f)
                curveTo(57.6242f, 41.1057f, 58.4739f, 42.4669f, 59.0848f, 43.9877f)
                curveTo(59.6956f, 45.5061f, 60.0034f, 47.0984f, 60.0034f, 48.7146f)
                curveTo(60.0034f, 50.1949f, 59.7202f, 51.7086f, 59.1611f, 53.2151f)
                curveTo(58.602f, 54.7193f, 57.7892f, 56.0875f, 56.7449f, 57.2842f)
                curveTo(55.7031f, 58.476f, 54.4371f, 59.4557f, 52.9765f, 60.1947f)
                curveTo(51.5406f, 60.9241f, 49.9224f, 61.2936f, 48.1663f, 61.2936f)
                close()
                moveTo(58.6192f, 38.486f)
                curveTo(57.368f, 37.0725f, 55.8484f, 35.902f, 54.1021f, 35.0105f)
                curveTo(52.3312f, 34.1071f, 50.3337f, 33.6494f, 48.1663f, 33.6494f)
                curveTo(46.191f, 33.6494f, 44.3043f, 34.057f, 42.5581f, 34.8651f)
                curveTo(40.8241f, 35.6661f, 39.2921f, 36.7721f, 38.004f, 38.1499f)
                curveTo(36.7183f, 39.5229f, 35.6888f, 41.1391f, 34.9425f, 42.9555f)
                curveTo(34.1937f, 44.7719f, 33.8144f, 46.7099f, 33.8144f, 48.7146f)
                curveTo(33.8144f, 50.6073f, 34.1642f, 52.4619f, 34.8563f, 54.2234f)
                curveTo(35.5435f, 55.9826f, 36.5262f, 57.5797f, 37.7725f, 58.9718f)
                curveTo(39.0237f, 60.3711f, 40.5458f, 61.5129f, 42.2945f, 62.3687f)
                curveTo(44.058f, 63.2316f, 46.0309f, 63.6892f, 48.154f, 63.725f)
                horizontalLineTo(48.1786f)
                curveTo(50.1884f, 63.6892f, 52.0899f, 63.2506f, 53.8336f, 62.4283f)
                curveTo(55.5627f, 61.6106f, 57.0947f, 60.495f, 58.3853f, 59.1172f)
                curveTo(59.6685f, 57.7442f, 60.6882f, 56.1447f, 61.4172f, 54.3665f)
                curveTo(62.1463f, 52.581f, 62.5157f, 50.6788f, 62.5157f, 48.7146f)
                curveTo(62.5157f, 46.934f, 62.1758f, 45.1199f, 61.5083f, 43.3226f)
                curveTo(60.8384f, 41.5253f, 59.8655f, 39.8972f, 58.6192f, 38.486f)
                close()

                moveTo(75.5252f, 60.5855f)
                curveTo(75.1041f, 60.6889f, 74.7321f, 60.7658f, 74.4218f, 60.8163f)
                curveTo(74.0942f, 60.8716f, 73.742f, 60.9245f, 73.3603f, 60.9774f)
                curveTo(73.008f, 61.0255f, 72.6608f, 61.052f, 72.3258f, 61.052f)
                curveTo(71.4835f, 61.052f, 70.8628f, 60.7779f, 70.4293f, 60.2176f)
                curveTo(69.949f, 59.5997f, 69.715f, 58.9842f, 69.715f, 58.3398f)
                verticalLineTo(22.4329f)
                horizontalLineTo(67.2028f)
                verticalLineTo(58.3398f)
                curveTo(67.2028f, 59.6863f, 67.6387f, 60.8789f, 68.4983f, 61.8815f)
                curveTo(69.3899f, 62.9226f, 70.6584f, 63.4491f, 72.2716f, 63.4491f)
                curveTo(72.7765f, 63.4491f, 73.279f, 63.4203f, 73.7642f, 63.365f)
                curveTo(74.2322f, 63.3097f, 74.6509f, 63.2544f, 75.0277f, 63.1991f)
                curveTo(75.4119f, 63.1438f, 75.8577f, 63.0572f, 76.3528f, 62.9466f)
                lineTo(77.205f, 62.7543f)
                lineTo(76.0868f, 60.4485f)
                lineTo(75.5252f, 60.5855f)
                close()

                moveTo(86.3673f, 36.1095f)
                curveTo(85.0619f, 36.9976f, 83.9068f, 38.1263f, 82.9191f, 39.4764f)
                verticalLineTo(34.0903f)
                horizontalLineTo(80.4069f)
                verticalLineTo(63.1733f)
                horizontalLineTo(82.9191f)
                verticalLineTo(45.0672f)
                curveTo(83.2664f, 43.8814f, 83.7467f, 42.7671f, 84.3452f, 41.7599f)
                curveTo(84.9487f, 40.7431f, 85.6974f, 39.8478f, 86.5742f, 39.1001f)
                curveTo(87.4511f, 38.3525f, 88.4707f, 37.7477f, 89.6086f, 37.2976f)
                curveTo(90.7416f, 36.8476f, 92.0224f, 36.5857f, 93.4115f, 36.519f)
                lineTo(94.079f, 36.4857f)
                verticalLineTo(34.0903f)
                horizontalLineTo(93.377f)
                curveTo(90.6973f, 34.0903f, 88.3402f, 34.7689f, 86.3673f, 36.1095f)
                close()
            }
        }.build(),
    )

    val sunPainter = rememberVectorPainter(
        ImageVector.Builder(
            name = "Logos.SolrLogo.Sun",
            defaultWidth = 128.dp,
            defaultHeight = 64.dp,
            viewportWidth = 128f,
            viewportHeight = 64f,
            autoMirror = false,
        ).apply {
            materialPath {
                moveTo(122.265f, 6.69195f)
                lineTo(101.458f, 28.1865f)
                lineTo(127.608f, 16.659f)
                curveTo(126.834f, 12.844f, 124.94f, 9.41495f, 122.265f, 6.69195f)
                close()

                moveTo(106.827f, 0f)
                curveTo(103.946f, 0f, 101.199f, 0.555789f, 98.6946f, 1.55857f)
                lineTo(95.9016f, 24.1496f)
                lineTo(109.854f, 0.21049f)
                curveTo(108.864f, 0.073317f, 107.857f, 0f, 106.827f, 0f)
                close()

                moveTo(127.817f, 18.4183f)
                lineTo(103.037f, 31.2849f)
                lineTo(126.285f, 28.7253f)
                curveTo(127.388f, 26.3462f, 127.999f, 23.7272f, 127.999f, 20.9733f)
                curveTo(127.999f, 20.1072f, 127.933f, 19.2547f, 127.817f, 18.4183f)
                close()

                moveTo(118.682f, 37.7183f)
                curveTo(121.45f, 36.0998f, 123.748f, 33.9305f, 125.371f, 31.3933f)
                lineTo(103.581f, 35.1279f)
                lineTo(118.682f, 37.7183f)
                close()

                moveTo(111.13f, 0.438413f)
                lineTo(99f, 25.7284f)
                lineTo(121.499f, 5.70775f)
                curveTo(118.679f, 3.09678f, 115.11f, 1.22976f, 111.13f, 0.438413f)
                close()

                moveTo(108.598f, 41.265f)
                curveTo(110.943f, 41.1329f, 113.177f, 40.7452f, 115.243f, 40.1369f)
                lineTo(103.037f, 39.1542f)
                lineTo(108.598f, 41.265f)
                close()

                moveTo(87.5299f, 12.4677f)
                curveTo(86.5767f, 14.4097f, 85.9585f, 16.5197f, 85.7442f, 18.7363f)
                lineTo(89.0323f, 24.1496f)
                lineTo(87.5299f, 12.4677f)
                close()

                moveTo(96.8597f, 2.49255f)
                curveTo(93.8721f, 4.01874f, 91.3155f, 6.20574f, 89.3993f, 8.85365f)
                lineTo(92.4682f, 23.6053f)
                lineTo(96.8597f, 2.49255f)
                close()
            }
        }.build(),
    )

    Box(modifier) {
        Icon(
            painter = solrPainter,
            tint = MaterialTheme.colorScheme.onBackground,
            contentDescription = stringResource(Res.string.cd_solr_logo),
        )

        Icon(
            painter = sunPainter,
            tint = MaterialTheme.colorScheme.primaryContainer,
            contentDescription = stringResource(Res.string.cd_solr_logo),
        )
    }
}
