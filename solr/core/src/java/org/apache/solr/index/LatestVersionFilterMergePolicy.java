package org.apache.solr.index;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Version;

/**
 * Only allows latest version segments to be considered for merges. That way a snapshot of older
 * segments can remain consistent
 */
public class LatestVersionFilterMergePolicy extends MergePolicy {
  MergePolicy delegatePolicy = new TieredMergePolicy();

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    /*we don't want to remove from the original SegmentInfos, else the segments may not carry forward upon a commit.
    That would be catastrophic. Hence we clone.*/
    SegmentInfos infosClone = infos.clone();
    infosClone.clear();
    for (SegmentCommitInfo info : infos) {
      if (info.info.getMinVersion() != null
          && info.info.getMinVersion().major == Version.LATEST.major) {
        infosClone.add(info);
      }
    }

    return delegatePolicy.findMerges(mergeTrigger, infosClone, mergeContext);
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    return delegatePolicy.findForcedMerges(
        segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    return delegatePolicy.findForcedDeletesMerges(segmentInfos, mergeContext);
  }
}
