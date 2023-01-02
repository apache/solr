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

import java.lang.invoke.MethodHandles;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MergePolicyFactory} for simple {@link MergePolicy} objects. Implementations need only
 * create the policy {@link #getMergePolicyInstance() instance} and this class will then configure
 * it with all set properties.
 */
public abstract class SimpleMergePolicyFactory extends MergePolicyFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected SimpleMergePolicyFactory(
      SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
  }

  protected abstract MergePolicy getMergePolicyInstance();

  @Override
  public final MergePolicy getMergePolicy() {
    final MergePolicy mp = getMergePolicyInstance();
    if (mp instanceof NoMergePolicy) { // allow turning off merges in config via system prop
      try {
        args.invokeSetters(mp);
      } catch (RuntimeException e) {
        String msg = e.getMessage();
        if (log.isInfoEnabled()) {
          log.info(
              "Ignoring unknown config setting for {} : {}",
              NoMergePolicy.class.getSimpleName(),
              msg);
        }
      }
    } else {
      args.invokeSetters(mp);
    }

    return mp;
  }
}
