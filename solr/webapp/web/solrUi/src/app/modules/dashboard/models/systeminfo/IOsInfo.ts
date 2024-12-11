/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version number.number
 * (the License); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-number.number
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export interface IOsInfo {
  name: string;
  arch: string;
  availableProcessors: number;
  systemLoadAverage: number;
  version: string;
  committedVirtualMemorySize: number;
  cpuLoad: number;
  freeMemorySize: number;
  freePhysicalMemorySize: number;
  freeSwapSpaceSize: number;
  processCpuLoad: number;
  processCpuTime: number;
  systemCpuLoad: number;
  totalMemorySize: number;
  totalPhysicalMemorySize: number;
  totalSwapSpaceSize: number;
  maxFileDescriptorCount: number;
  openFileDescriptorCount: number;
}
