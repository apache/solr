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

package org.apache.solr.ui.components.environment.data

import kotlinx.serialization.Serializable

@Serializable
data class SystemInformation(
    val name: String = "",
    val arch: String = "",
    val availableProcessors: Int = 0,
    val systemLoadAverage: Double = 0.0,
    val version: String = "",
    val committedVirtualMemorySize: Long = 0,
    val cpuLoad: Float = 0f,
    val freeMemorySize: Long = 0,
    val freePhysicalMemorySize: Long = 0,
    val freeSwapSpaceSize: Long = 0,
    val processCpuLoad: Float = 0f,
    val processCpuTime: Long = 0,
    val systemCpuLoad: Float = 0f,
    val totalMemorySize: Long = 0,
    val totalPhysicalMemorySize: Long = 0,
    val totalSwapSpaceSize: Long = 0,
    val maxFileDescriptorCount: Long = 0,
    val openFileDescriptorCount: Long = 0,
)
