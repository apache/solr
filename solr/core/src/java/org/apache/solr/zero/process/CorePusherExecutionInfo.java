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

package org.apache.solr.zero.process;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.solr.zero.client.ZeroFile;

public class CorePusherExecutionInfo {

  private final boolean hasPushed;
  private final long zeroGeneration;
  private final String metadataSuffix;
  private final Collection<ZeroFile.WithLocal> filesToDelete;
  private final Collection<ZeroFile.WithLocal> filesToPush;
  private final long pushLockWaitTimeMs;
  private final long actualPushTimeMs;
  private final long metadataUpdateTimeMs;
  private final long totalTimeMs;

  public CorePusherExecutionInfo(
      boolean hasPushed,
      long zeroGeneration,
      String metadataSuffix,
      Collection<ZeroFile.WithLocal> filesToPush,
      Collection<ZeroFile.WithLocal> filesToDelete,
      long pushLockWaitTimeMs,
      long actualPushTimeMs,
      long metadataUpdateTimeMs,
      long totalTimeMs) {
    this.hasPushed = hasPushed;
    this.zeroGeneration = zeroGeneration;
    this.metadataSuffix = metadataSuffix;
    this.filesToPush = filesToPush;
    this.filesToDelete = filesToDelete;
    this.pushLockWaitTimeMs = pushLockWaitTimeMs;
    this.actualPushTimeMs = actualPushTimeMs;
    this.metadataUpdateTimeMs = metadataUpdateTimeMs;
    this.totalTimeMs = totalTimeMs;
  }

  public static CorePusherExecutionInfo localAlreadyUpToDate(
      long zeroGeneration, long pushLockWaitTimeMs, long totalTimeMs) {
    return new CorePusherExecutionInfo(
        false,
        zeroGeneration,
        null,
        new ArrayList<>(),
        new ArrayList<>(),
        pushLockWaitTimeMs,
        0,
        0,
        totalTimeMs);
  }

  public static CorePusherExecutionInfo oddEmptyDiffCommitPoint(
      long zeroGeneration,
      Collection<ZeroFile.WithLocal> filesToPush,
      Collection<ZeroFile.WithLocal> filesToDelete,
      long pushLockWaitTimeMs,
      long totalTimeMs) {
    return new CorePusherExecutionInfo(
        false,
        zeroGeneration,
        null,
        filesToPush,
        filesToDelete,
        pushLockWaitTimeMs,
        0,
        0,
        totalTimeMs);
  }

  public boolean hasPushed() {
    return hasPushed;
  }

  public long getZeroGeneration() {
    return zeroGeneration;
  }

  public String getMetadataSuffix() {
    return metadataSuffix;
  }

  public long getPushLockWaitTimeMs() {
    return pushLockWaitTimeMs;
  }

  public long getActualPushTimeMs() {
    return actualPushTimeMs;
  }

  public long getMetadataUpdateTimeMs() {
    return metadataUpdateTimeMs;
  }

  public long getTotalTimeMs() {
    return totalTimeMs;
  }

  public int getNumFilesPushed() {
    return filesToPush.size();
  }

  public long getSizeBytesPushed() {
    return filesToPush.stream().mapToLong(ZeroFile.WithLocal::getFileSize).sum();
  }

  public int getNumFilesToDelete() {
    return filesToDelete.size();
  }

  @Override
  public String toString() {
    return "ExecutionInfo [hasPushed="
        + hasPushed
        + ", zeroGeneration="
        + zeroGeneration
        + ", metadataSuffix="
        + metadataSuffix
        + ", filesToPush="
        + filesToPush
        + ", filesToDelete="
        + filesToDelete
        + ", pushLockWaitTimeMs="
        + pushLockWaitTimeMs
        + ", actualPushTimeMs="
        + actualPushTimeMs
        + ", metadataUpdateTimeMs="
        + metadataUpdateTimeMs
        + ", totalTimeMs="
        + totalTimeMs
        + "]";
  }
}
