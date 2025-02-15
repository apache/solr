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
package org.apache.solr.util;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.index.MergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.index.MergePolicyFactory;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.junit.rules.TestRule;

/**
 * A {@link MergePolicyFactory} for {@link RandomMergePolicy} preventing random segment reversing.
 * It's absolutely necessary for all block join dependent tests. Without it, they may unexpectedly
 * fail from time to time.
 */
public final class RandomNoReverseMergePolicyFactory extends MergePolicyFactory {

  /**
   * This rule works because all solrconfig*.xml files include
   * solrconfig.snippet.randomindexconfig.xml where this property is used. If one refuse to include
   * it, test may unexpectedly fail from time to time.
   */
  public static TestRule createRule() {
    return new SystemPropertiesRestoreRule(
        SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY,
        RandomNoReverseMergePolicyFactory.class.getName());
  }

  public RandomNoReverseMergePolicyFactory() {
    super(null, new MergePolicyFactoryArgs(), null);
  }

  @Override
  public MergePolicy getMergePolicy() {
    final boolean allowReverse = false;
    return new RandomMergePolicy(allowReverse);
  }
}
