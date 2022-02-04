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

package org.apache.solr.jaeger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import io.opentracing.util.GlobalTracer;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

@ThreadLeakLingering(linger = 10)
public class TestJaegerConfigurator extends SolrTestCaseJ4 {

  @Rule public TestRule solrTestRules = new SystemPropertiesRestoreRule();

  @Before
  public void doBefore() {
    // to be safe because this test tests tracing.
    resetGlobalTracer();
    ExecutorUtil.resetThreadLocalProviders();

    // The default sampler is one that uses GSON but we don't bother with that dependency.
    //  So we change it to something simple.
    System.setProperty("JAEGER_SAMPLER_TYPE", "const");
  }

  @Test
  public void testInjected() throws Exception {
    MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster.Builder(2, createTempDir())
            .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
            .withSolrXml(getFile("solr/solr.xml").toPath())
            .build();
    try {
      TimeOut timeOut = new TimeOut(2, TimeUnit.MINUTES, TimeSource.NANO_TIME);
      timeOut.waitFor(
          "Waiting for GlobalTracer is registered",
          () -> GlobalTracer.get().toString().contains("JaegerTracer"));

      // TODO add run Jaeger through Docker and verify spans available after run these commands
      CollectionAdminRequest.createCollection("test", 2, 1).process(cluster.getSolrClient());
      new UpdateRequest().add("id", "1").add("id", "2").process(cluster.getSolrClient(), "test");
      cluster.getSolrClient().query("test", new SolrQuery("*:*"));
    } finally {
      cluster.shutdown();
    }
  }
}
