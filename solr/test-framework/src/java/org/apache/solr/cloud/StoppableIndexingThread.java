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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoppableIndexingThread extends AbstractFullDistribZkTestBase.StoppableThread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static String t1 = "a_t";
  static String i1 = "a_i";
  private volatile boolean stop = false;
  protected final String id;
  protected final List<String> deletes = new ArrayList<>();
  protected Set<String> addFails = new HashSet<>();
  protected Set<String> deleteFails = new HashSet<>();
  protected boolean doDeletes;
  private int numCycles;
  private SolrClient controlClient;
  private SolrClient cloudClient;
  private int numDeletes;
  private int numAdds;
  private List<SolrInputDocument> docs = new ArrayList<>();
  private int batchSize;
  private boolean pauseBetweenUpdates;

  public StoppableIndexingThread(
      SolrClient controlClient, SolrClient cloudClient, String id, boolean doDeletes) {
    this(controlClient, cloudClient, id, doDeletes, -1, 1, true);
  }

  public StoppableIndexingThread(
      SolrClient controlClient,
      SolrClient cloudClient,
      String id,
      boolean doDeletes,
      int numCycles,
      int batchSize,
      boolean pauseBetweenUpdates) {
    super("StoppableIndexingThread");
    this.controlClient = controlClient;
    this.cloudClient = cloudClient;
    this.id = id;
    this.doDeletes = doDeletes;
    this.numCycles = numCycles;
    this.batchSize = batchSize;
    this.pauseBetweenUpdates = pauseBetweenUpdates;
    setDaemon(true);
  }

  @Override
  public void run() {
    int i = 0;
    int numDone = 0;
    numDeletes = 0;
    numAdds = 0;

    while (true && !stop) {
      if (numCycles != -1) {
        if (numDone > numCycles) {
          break;
        }
      }
      ++numDone;
      String id = this.id + "-" + i;
      ++i;
      boolean addFailed = false;

      if (doDeletes && AbstractFullDistribZkTestBase.random().nextBoolean() && deletes.size() > 0) {
        String deleteId = deletes.remove(0);
        try {
          numDeletes++;
          if (controlClient != null) {
            UpdateRequest req = new UpdateRequest();
            req.deleteById(deleteId);
            req.setParam("CONTROL", "TRUE");
            req.process(controlClient);
          }

          cloudClient.deleteById(deleteId);
        } catch (Exception e) {
          log.error("REQUEST FAILED for id={}", deleteId, e);
          if (e instanceof SolrServerException) {
            log.error("ROOT CAUSE for id={}", deleteId, ((SolrServerException) e).getRootCause());
          }
          deleteFails.add(deleteId);
        }
      }

      try {
        numAdds++;
        SolrInputDocument doc = new SolrInputDocument();
        addFields(doc, "id", id, i1, 50, t1, "to come to the aid of their country.");
        addFields(doc, "rnd_b", true);

        docs.add(doc);

        if (docs.size() >= batchSize) {
          indexDocs(docs);
          docs.clear();
        }
      } catch (Exception e) {
        addFailed = true;
        log.error("REQUEST FAILED for id={}", id, e);
        if (e instanceof SolrServerException) {
          log.error("ROOT CAUSE for id={}", id, ((SolrServerException) e).getRootCause());
        }
        addFails.add(id);
      }

      if (!addFailed && doDeletes && AbstractFullDistribZkTestBase.random().nextBoolean()) {
        deletes.add(id);
      }

      if (docs.size() > 0 && pauseBetweenUpdates) {
        try {
          Thread.sleep(AbstractFullDistribZkTestBase.random().nextInt(500) + 50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    System.err.println(
        "added docs:"
            + numAdds
            + " with "
            + (addFails.size() + deleteFails.size())
            + " fails"
            + " deletes:"
            + numDeletes);
  }

  @Override
  public void safeStop() {
    stop = true;
  }

  public Set<String> getAddFails() {
    return addFails;
  }

  public Set<String> getDeleteFails() {
    return deleteFails;
  }

  public int getFailCount() {
    return addFails.size() + deleteFails.size();
  }

  protected void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }

  protected void indexDocs(List<SolrInputDocument> docs) throws IOException, SolrServerException {

    if (controlClient != null) {
      UpdateRequest req = new UpdateRequest();
      req.add(docs);
      req.setParam("CONTROL", "TRUE");
      req.process(controlClient);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(docs);
    ureq.process(cloudClient);
  }

  public int getNumDeletes() {
    return numDeletes;
  }

  public int getNumAdds() {
    return numAdds;
  }
}
