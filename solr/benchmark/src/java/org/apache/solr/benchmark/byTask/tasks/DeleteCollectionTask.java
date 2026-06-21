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
package org.apache.solr.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;

import java.util.concurrent.atomic.AtomicInteger;

public class DeleteCollectionTask extends PerfTask {

  static AtomicInteger count = new AtomicInteger();

  private String name;

  public DeleteCollectionTask(PerfRunData runData) {
    super(runData);
  }


  @Override
  protected String getLogMessage(int recsCount) {
    return "collection created";
  }

  @Override
  public int doLogic() throws Exception {
    SolrClient solrServer = (SolrClient) getRunData().getPerfObject("solr.admin.client");
    String collectionName = name;
    CollectionAdminResponse response = CollectionAdminRequest
        .deleteCollection(collectionName).process(solrServer);

    return 1;
  }

  /**
   * Set the params (docSize only)
   *
   * @param params
   *          docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    // can't call super because super doesn't understand our
    // params syntax
    this.params = params;
    String [] splits = params.split(",");
    for (int i = 0; i < splits.length; i++) {
      if (splits[i].startsWith("name[") == true){
        name = splits[i].substring("name[".length(),splits[i].length() - 1);
      }
    }
  }

}
