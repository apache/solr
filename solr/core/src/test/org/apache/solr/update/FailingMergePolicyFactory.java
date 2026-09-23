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
package org.apache.solr.update;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.index.MergePolicyFactory;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.schema.IndexSchema;

/**
 * Test-only merge policy that starts each Lucene merge, then throws so {@link SolrIndexWriter} can
 * exercise its failed-merge metrics path.
 */
public class FailingMergePolicyFactory extends MergePolicyFactory {

  public static final String INJECTED_FAILURE = "injected merge failure";

  public FailingMergePolicyFactory(
      SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
  }

  @Override
  public MergePolicy getMergePolicy() {
    TieredMergePolicy inner = new TieredMergePolicy();
    args.invokeSetters(inner);
    return new FailingMergePolicy(inner);
  }

  static final class FailingMergePolicy extends FilterMergePolicy {
    FailingMergePolicy(MergePolicy in) {
      super(in);
    }

    @Override
    public MergeSpecification findMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      return wrap(super.findMerges(mergeTrigger, segmentInfos, mergeContext));
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos segmentInfos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext)
        throws IOException {
      return wrap(
          super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext));
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(
        SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
      return wrap(super.findForcedDeletesMerges(segmentInfos, mergeContext));
    }

    @Override
    public MergeSpecification findFullFlushMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      return wrap(super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext));
    }

    private static MergeSpecification wrap(MergeSpecification spec) {
      if (spec == null) {
        return null;
      }
      MergeSpecification failing = new MergeSpecification();
      for (OneMerge merge : spec.merges) {
        failing.add(new FailingOneMerge(merge));
      }
      return failing;
    }
  }

  static final class FailingOneMerge extends MergePolicy.OneMerge {
    FailingOneMerge(MergePolicy.OneMerge merge) {
      super(merge);
    }

    @Override
    public void mergeInit() throws IOException {
      super.mergeInit();
      throw new IOException(INJECTED_FAILURE);
    }
  }
}
