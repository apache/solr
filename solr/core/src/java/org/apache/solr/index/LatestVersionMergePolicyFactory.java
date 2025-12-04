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

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;

/**
 * A {@link MergePolicyFactory} for {@link LatestVersionFilterMergePolicy} objects. The returned
 * LatestVersionFilterMergePolicy instance blocks older version segments (&lt; current version of
 * Lucene) from participating in merges and delegates the merging to a TieredMergePolicy instance by
 * default. This can be used to reindex the data and ensure all segments are the latest version
 * segments by the end of the reindexing. This can help prepare the index for upgrade to a later
 * version of Solr/Lucene even if it was initially created on a now unsupported version
 */
public class LatestVersionMergePolicyFactory extends SimpleMergePolicyFactory {

  public LatestVersionMergePolicyFactory(
      SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
  }

  @Override
  protected MergePolicy getMergePolicyInstance() {
    return new LatestVersionFilterMergePolicy(new TieredMergePolicy());
  }
}
