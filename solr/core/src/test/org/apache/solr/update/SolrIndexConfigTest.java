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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.misc.index.BPReorderingMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.TestMergePolicyConfig;
import org.apache.solr.index.SortingMergePolicy;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testcase for {@link SolrIndexConfig}
 *
 * @see TestMergePolicyConfig
 */
public class SolrIndexConfigTest extends SolrTestCaseJ4 {

  private static final String solrConfigFileName = "solrconfig.xml";
  private static final String solrConfigFileNameWarmerRandomMergePolicyFactory =
      "solrconfig-warmer-randommergepolicyfactory.xml";
  private static final String solrConfigFileNameTieredMergePolicyFactory =
      "solrconfig-tieredmergepolicyfactory.xml";
  private static final String solrConfigFileNameConnMSPolicyFactory =
      "solrconfig-concurrentmergescheduler.xml";
  private static final String solrConfigFileNameSortingMergePolicyFactory =
      "solrconfig-sortingmergepolicyfactory.xml";
  private static final String solrConfigFileNameBPReorderingMergePolicyFactory =
      "solrconfig-bpreorderingmergepolicyfactory.xml";
  private static final String schemaFileName = "schema.xml";

  private static boolean compoundMergePolicySort = false;

  @BeforeClass
  public static void beforeClass() throws Exception {
    compoundMergePolicySort = random().nextBoolean();
    if (compoundMergePolicySort) {
      System.setProperty("mergePolicySort", "timestamp_i_dvo desc, id asc");
    }
    initCore(solrConfigFileName, schemaFileName);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("mergePolicySort");
    System.clearProperty("solr.tests.maxCommitMergeWait");
    super.tearDown();
  }

  private final Path instanceDir = TEST_PATH().resolve("collection1");

  @Test
  public void testFailingSolrIndexConfigCreation() throws Exception {
    SolrConfig solrConfig = new SolrConfig(instanceDir, "bad-mpf-solrconfig.xml");
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);
    h.getCore().setLatestSchema(indexSchema);

    // this should fail as mergePolicy doesn't have any public constructor
    SolrException ex =
        expectThrows(SolrException.class, () -> solrIndexConfig.toIndexWriterConfig(h.getCore()));
    assertTrue(
        ex.getMessage()
            .contains(
                "Error instantiating class: 'org.apache.solr.index.DummyMergePolicyFactory'"));
  }

  @Test
  public void testTieredMPSolrIndexConfigCreation() throws Exception {
    String solrConfigFileName = solrConfigFileNameTieredMergePolicyFactory;
    SolrConfig solrConfig = new SolrConfig(instanceDir, solrConfigFileName);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);

    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());

    assertNotNull("null mp", iwc.getMergePolicy());
    assertTrue("mp is not TieredMergePolicy", iwc.getMergePolicy() instanceof TieredMergePolicy);
    TieredMergePolicy mp = (TieredMergePolicy) iwc.getMergePolicy();
    assertEquals("mp.maxMergeAtOnce", 7, mp.getMaxMergeAtOnce());
    assertEquals("mp.segmentsPerTier", 9, (int) mp.getSegmentsPerTier());

    assertNotNull("null ms", iwc.getMergeScheduler());
    assertTrue("ms is not CMS", iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler);
    ConcurrentMergeScheduler ms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
    assertEquals("ms.maxMergeCount", 987, ms.getMaxMergeCount());
    assertEquals("ms.maxThreadCount", 42, ms.getMaxThreadCount());
    assertTrue("ms.isAutoIOThrottle", ms.getAutoIOThrottle());
  }

  @Test
  public void testConcurrentMergeSchedularSolrIndexConfigCreation() throws Exception {
    String solrConfigFileName = solrConfigFileNameConnMSPolicyFactory;
    SolrConfig solrConfig = new SolrConfig(instanceDir, solrConfigFileName);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);

    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());

    assertNotNull("null mp", iwc.getMergePolicy());
    assertTrue("mp is not TieredMergePolicy", iwc.getMergePolicy() instanceof TieredMergePolicy);

    assertNotNull("null ms", iwc.getMergeScheduler());
    assertTrue("ms is not CMS", iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler);
    ConcurrentMergeScheduler ms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
    assertEquals("ms.maxMergeCount", 987, ms.getMaxMergeCount());
    assertEquals("ms.maxThreadCount", 42, ms.getMaxThreadCount());
    assertFalse("ms.isAutoIOThrottle", ms.getAutoIOThrottle());
  }

  public void testSortingMPSolrIndexConfigCreation() throws Exception {
    final SortField sortField1 = new SortField("timestamp_i_dvo", SortField.Type.INT, true);
    final SortField sortField2 = new SortField("id", SortField.Type.STRING, false);
    sortField2.setMissingValue(SortField.STRING_LAST);

    SolrConfig solrConfig =
        new SolrConfig(instanceDir, solrConfigFileNameSortingMergePolicyFactory);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    assertNotNull(solrIndexConfig);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);

    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());

    final MergePolicy mergePolicy = iwc.getMergePolicy();
    assertNotNull("null mergePolicy", mergePolicy);
    assertTrue(
        "mergePolicy (" + mergePolicy + ") is not a SortingMergePolicy",
        mergePolicy instanceof SortingMergePolicy);
    final SortingMergePolicy sortingMergePolicy = (SortingMergePolicy) mergePolicy;
    final Sort expected;
    if (compoundMergePolicySort) {
      expected = new Sort(sortField1, sortField2);
    } else {
      expected = new Sort(sortField1);
    }
    final Sort actual = sortingMergePolicy.getSort();
    assertEquals("SortingMergePolicy.getSort", expected, actual);
  }

  public void testBPReorderingMPSolrIndexConfigCreation() throws Exception {
    SolrConfig solrConfig =
        new SolrConfig(instanceDir, solrConfigFileNameBPReorderingMergePolicyFactory);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    assertNotNull(solrIndexConfig);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);

    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());

    final MergePolicy mergePolicy = iwc.getMergePolicy();
    assertNotNull("null mergePolicy", mergePolicy);
    assertTrue(
        "mergePolicy (" + mergePolicy + ") is not a BPReorderingMergePolicy",
        mergePolicy instanceof BPReorderingMergePolicy);
  }

  public void testMergedSegmentWarmerIndexConfigCreation() throws Exception {
    SolrConfig solrConfig =
        new SolrConfig(instanceDir, solrConfigFileNameWarmerRandomMergePolicyFactory);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    assertNotNull(solrIndexConfig);
    assertNotNull(solrIndexConfig.mergedSegmentWarmerInfo);
    assertEquals(
        SimpleMergedSegmentWarmer.class.getName(),
        solrIndexConfig.mergedSegmentWarmerInfo.className);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);
    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());
    assertEquals(SimpleMergedSegmentWarmer.class, iwc.getMergedSegmentWarmer().getClass());
  }

  public void testToMap() throws Exception {
    final String solrConfigFileNameWarmer = solrConfigFileNameWarmerRandomMergePolicyFactory;
    final String solrConfigFileNameTMP = solrConfigFileNameTieredMergePolicyFactory;
    final String solrConfigFileName =
        (random().nextBoolean() ? solrConfigFileNameWarmer : solrConfigFileNameTMP);
    SolrConfig solrConfig = new SolrConfig(instanceDir, solrConfigFileName);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null);
    assertNotNull(solrIndexConfig);
    assertNotNull(solrIndexConfig.mergePolicyFactoryInfo);
    if (solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      assertNotNull(solrIndexConfig.mergedSegmentWarmerInfo);
    } else {
      assertNull(solrIndexConfig.mergedSegmentWarmerInfo);
    }
    assertNotNull(solrIndexConfig.mergeSchedulerInfo);

    Map<String, Object> m = solrIndexConfig.toMap(new LinkedHashMap<>());
    int mSizeExpected = 0;

    ++mSizeExpected;
    assertTrue(m.get("useCompoundFile") instanceof Boolean);

    ++mSizeExpected;
    assertTrue(m.get("maxBufferedDocs") instanceof Integer);

    ++mSizeExpected;
    assertTrue(m.get("ramBufferSizeMB") instanceof Double);

    ++mSizeExpected;
    assertTrue(m.get("maxCommitMergeWaitTime") instanceof Integer);

    ++mSizeExpected;
    assertTrue(m.get("ramPerThreadHardLimitMB") instanceof Integer);

    ++mSizeExpected;
    assertTrue(m.get("writeLockTimeout") instanceof Integer);

    ++mSizeExpected;
    assertTrue(m.get("lockType") instanceof String);
    {
      final String lockType = (String) m.get("lockType");
      assertTrue(
          DirectoryFactory.LOCK_TYPE_SIMPLE.equals(lockType)
              || DirectoryFactory.LOCK_TYPE_NATIVE.equals(lockType)
              || DirectoryFactory.LOCK_TYPE_SINGLE.equals(lockType)
              || DirectoryFactory.LOCK_TYPE_NONE.equals(lockType));
    }

    ++mSizeExpected;
    assertTrue(m.get("infoStreamEnabled") instanceof Boolean);
    {
      assertFalse(Boolean.valueOf(m.get("infoStreamEnabled").toString()));
    }

    ++mSizeExpected;
    assertTrue(m.get("mergeScheduler") instanceof MapSerializable);
    ++mSizeExpected;
    assertTrue(m.get("mergePolicyFactory") instanceof MapSerializable);
    if (solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      ++mSizeExpected;
      assertTrue(m.get("mergedSegmentWarmer") instanceof MapSerializable);
    } else {
      assertNull(m.get("mergedSegmentWarmer"));
    }
    ++mSizeExpected;
    assertNotNull(m.get("metrics"));

    assertEquals(mSizeExpected, m.size());
  }

  public void testMaxCommitMergeWaitTime() throws Exception {
    SolrConfig sc = new SolrConfig(TEST_PATH().resolve("collection1"), "solrconfig-test-misc.xml");
    assertEquals(-1, sc.indexConfig.maxCommitMergeWaitMillis);
    assertEquals(
        IndexWriterConfig.DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS,
        sc.indexConfig.toIndexWriterConfig(h.getCore()).getMaxFullFlushMergeWaitMillis());
    System.setProperty("solr.tests.maxCommitMergeWaitTime", "10");
    sc = new SolrConfig(TEST_PATH().resolve("collection1"), "solrconfig-test-misc.xml");
    assertEquals(10, sc.indexConfig.maxCommitMergeWaitMillis);
    assertEquals(
        10, sc.indexConfig.toIndexWriterConfig(h.getCore()).getMaxFullFlushMergeWaitMillis());
  }
}
