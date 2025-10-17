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

import java.util.Set;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.misc.index.BPIndexReorderer;
import org.apache.lucene.misc.index.BPReorderingMergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;

/** A {@link MergePolicyFactory} for {@code BPReorderingMergePolicy} objects. */
public class BPReorderingMergePolicyFactory extends WrapperMergePolicyFactory {

  private static final String BPR_PREFIX = "bpr.prefix";

  private final BPIndexReorderer reorderer;

  public BPReorderingMergePolicyFactory(
      SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    reorderer = new BPIndexReorderer();
    MergePolicyFactoryArgs bprArgs = filterWrappedMergePolicyFactoryArgs(BPR_PREFIX);
    if (bprArgs != null) {
      final String fields = (String) bprArgs.remove("fields");
      if (fields != null) {
        reorderer.setFields(Set.of(fields.split(",")));
      }
      SolrPluginUtils.invokeSetters(reorderer, bprArgs.args.entrySet());
    }
  }

  @Override
  protected MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
    final MergePolicy mp = new BPReorderingMergePolicy(wrappedMP, reorderer);
    return mp;
  }
}
