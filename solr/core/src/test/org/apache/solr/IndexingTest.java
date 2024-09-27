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
 */a
package org.apache.solr;

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LogLevel(
    "org.eclipse.jetty.http2=DEBUG;org.eclipse.jetty.http2.hpack=INFO;org.eclipse.jetty.client.LEVEL=DEBUG")
public class IndexingTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testIndexing() throws Exception {
    final String collection = "test";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 15, TimeUnit.SECONDS, 1, 1);
    var jetty = cluster.getJettySolrRunner(random().nextInt(1));
    try (Http2SolrClient solrClient =
        new Http2SolrClient.Builder()
            .withIdleTimeout(100, TimeUnit.SECONDS)
            .withConnectionTimeout(100, TimeUnit.SECONDS)
            .build()) {
      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.setBasePath(jetty.getBaseUrl().toString());
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", 1);
      doc.addField(
          "text",
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec condimentum ornare mauris sit amet dictum. Vivamus viverra, leo nec dapibus interdum, enim velit vulputate mauris, ut consectetur sapien sapien quis ante. Aliquam et arcu posuere, sagittis tortor vitae, gravida nulla. Praesent sit amet tincidunt velit. Nullam vitae nunc vitae felis congue auctor id sit amet nulla. Suspendisse eget imperdiet risus, et laoreet mi. Aliquam iaculis justo a ex convallis vulputate.\n"
              + "\n"
              + "Nam nec malesuada eros. Aliquam id dui est. Aliquam rhoncus ex ac facilisis condimentum. Phasellus faucibus non urna id aliquam. Morbi efficitur, ipsum nec tincidunt bibendum, leo nisi viverra ante, in consequat tellus mi vitae diam. In hac habitasse platea dictumst. Quisque euismod dapibus nunc, ac ultricies mi viverra quis. Morbi lacinia odio nisl, eu vulputate eros vehicula eget. Etiam non lectus euismod sapien ornare efficitur eu ac justo. Aliquam erat volutpat. Praesent et lorem in nunc porta varius. Suspendisse scelerisque rhoncus augue, et dapibus mi. Aenean neque massa, accumsan sit amet eros quis, sodales tempus ex. Mauris pharetra sollicitudin lacinia. Maecenas sit amet urna eu massa elementum accumsan.\n"
              + "\n"
              + "Nulla posuere malesuada convallis. Maecenas molestie quam est, id mattis nisl feugiat nec. Nulla sapien mi, scelerisque vel posuere in, faucibus accumsan risus. Praesent eget tincidunt velit. Etiam lorem orci, dictum et molestie non, faucibus vel arcu. Nam hendrerit facilisis blandit. Fusce sit amet efficitur lacus. Etiam efficitur neque quis semper pulvinar.\n"
              + "\n");
      updateRequest.add(doc);
      System.out.println("1st Doc started!!!!!");
      solrClient.request(updateRequest, "test");
      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField("id", 2);
      doc1.addField("text", "Yo!");
      updateRequest = new UpdateRequest();
      updateRequest.setBasePath(jetty.getBaseUrl().toString());
      updateRequest.add(doc1);
      System.out.println("1st Doc started!!!!!");
      solrClient.request(updateRequest, "test");
    }
  }

  @Test
  public void testUpdate() throws Exception {
    String resp = "{responseHeader={rf=1, status=0, QTime=1}}";
    System.out.println(resp.getBytes().length);
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    shutdownCluster();
  }
}
