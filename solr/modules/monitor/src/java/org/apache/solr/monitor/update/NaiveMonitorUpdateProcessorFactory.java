/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.update;

import java.io.IOException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.monitor.search.NaiveReverseQueryParserPlugin;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

public class NaiveMonitorUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware {

  private NaiveReverseQueryParserPlugin reverseQueryParserPlugin = null;

  @Override
  public void inform(SolrCore core) {
    QParserPlugin plugin = core.getQueryPlugin(NaiveReverseQueryParserPlugin.NAME);
    if (plugin instanceof NaiveReverseQueryParserPlugin) {
      reverseQueryParserPlugin = (NaiveReverseQueryParserPlugin) plugin;
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {

      public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument solrInputDocument = cmd.getSolrInputDocument();
        reverseQueryParserPlugin.add(solrInputDocument);
        super.processAdd(cmd);
      }

      public void processDelete(DeleteUpdateCommand cmd) throws IOException {
        if (cmd.isDeleteById()) {
          reverseQueryParserPlugin.delete(cmd.getId());
        } else {
          // TODO
        }
        super.processDelete(cmd);
      }
    };
  }
}
