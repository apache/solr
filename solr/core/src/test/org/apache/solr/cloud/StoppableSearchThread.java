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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background search thread used by ChaosMonkey tests.
 * Continuously issues {@code *:*} queries against the cloud client.
 */
public class StoppableSearchThread extends StoppableThread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean stop = false;
  private final CloudHttp2SolrClient cloudClient;

  StoppableSearchThread(CloudHttp2SolrClient cloudClient) {
    super("StoppableSearchThread");
    this.cloudClient = cloudClient;
  }

  @Override
  public void run() {
    while (!stop) {
      try {
        cloudClient.query(new SolrQuery("*:*"));
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        // expected during chaos — log at debug and continue
        log.debug("Search failed (expected during chaos)", e);
      }
    }
    log.info("StoppableSearchThread done");
  }

  @Override
  public void safeStop() {
    stop = true;
  }
}
