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

package org.apache.solr.index;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;

/**
 * Prevents any older version segment (i.e. older than current lucene major version), either
 * original or one derived as a result of merging with an older version segment, from being
 * considered for merges. That way a snapshot of older segments remains consistent. This assists in
 * upgrading to a future Lucene major version if existing documents are reindexed in the current
 * version with this merge policy in place.
 */
public class LatestVersionFilterMergePolicy extends FilterMergePolicy {

  public LatestVersionFilterMergePolicy(MergePolicy in) {
    super(in);
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    return in.findMerges(mergeTrigger, getFilteredInfos(infos), mergeContext);
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos infos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    return in.findForcedMerges(
        getFilteredInfos(infos), maxSegmentCount, segmentsToMerge, mergeContext);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext)
      throws IOException {
    return in.findForcedDeletesMerges(getFilteredInfos(infos), mergeContext);
  }

  @Override
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    return in.findFullFlushMerges(mergeTrigger, getFilteredInfos(infos), mergeContext);
  }

  private SegmentInfos getFilteredInfos(SegmentInfos infos) {
    SegmentInfos infosClone = null;

    for (SegmentCommitInfo info : infos) {
      if (!allowSegmentForMerge(info)) {
        // There are older version segments present.
        // We should not remove from the original SegmentInfos. Hence we clone.
        infosClone = infos.clone();
        infosClone.clear();
        break;
      }
    }

    if (infosClone == null) {
      // All segments are latest major version and allowed to participate in merge
      return infos;
    } else {
      // Either mixed versions or all older version segments.
      // If we are here, most runs should fall in the former case.
      // The latter case should only happen once right after an upgrade, so we are ok with incurring
      // this redundant iteration for that one time to keep the logic simple
      for (SegmentCommitInfo info : infos) {
        if (allowSegmentForMerge(info)) {
          infosClone.add(info);
        }
      }
    }

    return infosClone;
  }

  /**
   * Determines if a SegmentCommitInfo should be part of the candidate set of segments that will be
   * considered for merges. By default, we only allow LATEST version segments to participate in
   * merges.
   */
  protected boolean allowSegmentForMerge(SegmentCommitInfo info) {
    return info.info.getMinVersion() != null
        && info.info.getMinVersion().major == Version.LATEST.major;
  }
}
