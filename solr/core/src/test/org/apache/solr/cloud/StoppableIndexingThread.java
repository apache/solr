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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background indexing thread used by ChaosMonkey tests.
 * Continuously adds (and optionally deletes) documents against the cloud client.
 */
public class StoppableIndexingThread extends StoppableThread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Field names matching schema15.xml (same as AbstractFullDistribZkTestBase)
  protected final String i1 = "a_i1";
  protected final String t1 = "a_t";

  protected volatile boolean stop = false;
  protected final SolrClient controlClient;
  protected final SolrClient cloudClient;
  protected final String id;
  protected final boolean doDeletes;
  protected final int maxUpdates;
  protected final int batchSize;
  protected final boolean pauseBetweenUpdates;

  protected volatile boolean useLongId = false;

  protected final List<String> deletes = new ArrayList<>();
  protected final AtomicInteger numAdded = new AtomicInteger();
  protected final AtomicInteger fails = new AtomicInteger();
  protected final Set<String> addFails = new HashSet<>();
  protected final Set<String> deleteFails = new HashSet<>();

  public StoppableIndexingThread(SolrClient controlClient, SolrClient cloudClient,
      String id, boolean doDeletes) {
    this(controlClient, cloudClient, id, doDeletes, -1, 1, true);
  }

  public StoppableIndexingThread(SolrClient controlClient, SolrClient cloudClient,
      String id, boolean doDeletes, int maxUpdates, int batchSize, boolean pauseBetweenUpdates) {
    super("StoppableIndexingThread-" + id);
    this.controlClient = controlClient;
    this.cloudClient = cloudClient;
    this.id = id;
    this.doDeletes = doDeletes;
    this.maxUpdates = maxUpdates;
    this.batchSize = batchSize;
    this.pauseBetweenUpdates = pauseBetweenUpdates;
  }

  @Override
  public void run() {
    int i = 0;
    while (!stop) {
      if (maxUpdates > 0 && numAdded.get() >= maxUpdates) {
        break;
      }
      String docId = id + "-" + i;
      ++i;

      if (doDeletes && SolrTestCase.random().nextBoolean() && !deletes.isEmpty()) {
        String toDelete = deletes.remove(0);
        try {
          if (controlClient != null) controlClient.deleteById(toDelete);
          cloudClient.deleteById(toDelete);
        } catch (Exception e) {
          log.warn("Delete failed for {}", toDelete, e);
          synchronized (deleteFails) { deleteFails.add(toDelete); }
          fails.incrementAndGet();
        }
      }

      try {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", docId);
        doc.addField(i1, SolrTestCase.random().nextInt(1000));
        doc.addField(t1, "document " + docId);

        if (batchSize == 1) {
          if (controlClient != null) controlClient.add(doc);
          cloudClient.add(doc);
        } else {
          // batch up to batchSize docs
          List<SolrInputDocument> batch = new ArrayList<>();
          batch.add(doc);
          for (int b = 1; b < batchSize && !stop; b++) {
            String bId = id + "-" + i;
            ++i;
            SolrInputDocument bdoc = new SolrInputDocument();
            bdoc.addField("id", bId);
            bdoc.addField(i1, SolrTestCase.random().nextInt(1000));
            bdoc.addField(t1, "document " + bId);
            batch.add(bdoc);
            if (doDeletes && SolrTestCase.random().nextBoolean()) {
              deletes.add(bId);
            }
          }
          if (controlClient != null) controlClient.add(batch);
          cloudClient.add(batch);
        }
        numAdded.incrementAndGet();

        if (doDeletes && SolrTestCase.random().nextBoolean()) {
          deletes.add(docId);
        }

        if (pauseBetweenUpdates) {
          Thread.sleep(LuceneTestCase.rarely() ? 500 : 10);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        log.warn("Add failed for {}", docId, e);
        synchronized (addFails) { addFails.add(docId); }
        fails.incrementAndGet();
      }
    }
    log.info("StoppableIndexingThread {} done: added={} fails={}", id, numAdded.get(), fails.get());
  }

  @Override
  public void safeStop() {
    stop = true;
  }

  public void setUseLongId(boolean useLongId) {
    this.useLongId = useLongId;
  }

  public int getFailCount() {
    return fails.get();
  }

  public Set<String> getAddFails() {
    synchronized (addFails) { return new HashSet<>(addFails); }
  }

  public Set<String> getDeleteFails() {
    synchronized (deleteFails) { return new HashSet<>(deleteFails); }
  }
}
