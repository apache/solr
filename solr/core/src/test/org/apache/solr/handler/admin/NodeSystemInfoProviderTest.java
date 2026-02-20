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
package org.apache.solr.handler.admin;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.Assert;
import org.junit.BeforeClass;

public class NodeSystemInfoProviderTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema.xml");
  }

  public void testMagickGetter() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    // make one directly
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    info.add("name", os.getName());
    info.add("version", os.getVersion());
    info.add("arch", os.getArch());

    // make another using MetricUtils.addMXBeanMetrics()
    SimpleOrderedMap<Object> info2 = new SimpleOrderedMap<>();
    SystemInfoProvider.forEachGetterValue(os, OperatingSystemMXBean.class, info2::add);

    // make sure they got the same thing
    for (String p : Arrays.asList("name", "version", "arch")) {
      assertEquals(info.get(p), info2.get(p));
    }
  }

  public void testGetNodeSystemInfo() {
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), new ModifiableSolrParams()) {};
    SystemInfoProvider provider = new SystemInfoProvider(req);
    NodeSystemResponse info = provider.getNodeSystemInfo(new NodeSystemResponse());

    Assert.assertNotNull(info.nodeInfo);
    // these can be validated
    Assert.assertEquals(h.getCoreContainer().getSolrHome().toString(), info.nodeInfo.solrHome);
    Assert.assertEquals(
        h.getCoreContainer().getCoreRootDirectory().toString(), info.nodeInfo.coreRoot);
    Assert.assertNotNull(info.nodeInfo.lucene);
    Assert.assertNotNull(info.nodeInfo.lucene.solrImplVersion);
    Assert.assertEquals(
        info.nodeInfo.lucene.solrImplVersion, SolrVersion.LATEST.getPrereleaseVersion());
    Assert.assertNotNull(info.nodeInfo.lucene.solrSpecVersion);
    Assert.assertEquals(info.nodeInfo.lucene.solrSpecVersion, SolrVersion.LATEST_STRING);
    Assert.assertNotNull(info.nodeInfo.lucene.luceneImplVersion);
    Assert.assertEquals(
        info.nodeInfo.lucene.luceneImplVersion, Version.getPackageImplementationVersion());
    Assert.assertNotNull(info.nodeInfo.lucene.luceneSpecVersion);
    Assert.assertEquals(info.nodeInfo.lucene.luceneSpecVersion, Version.LATEST.toString());
    // these should be set
    Assert.assertNotNull(info.nodeInfo.mode);
    Assert.assertNotNull(info.nodeInfo.jvm);
    Assert.assertNotNull(info.nodeInfo.security);
    Assert.assertNotNull(info.nodeInfo.system);
  }
}
