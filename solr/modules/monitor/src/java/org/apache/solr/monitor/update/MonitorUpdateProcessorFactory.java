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

import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.Presearcher;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.monitor.PresearcherFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

public class MonitorUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private Presearcher presearcher = PresearcherFactory.build();
  private String queryFieldNameOverride;
  private String payloadFieldNameOverride;

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    String queryFieldName =
        queryFieldNameOverride == null ? MonitorFields.MONITOR_QUERY : queryFieldNameOverride;
    String payloadFieldName =
        payloadFieldNameOverride == null ? MonitorFields.PAYLOAD : payloadFieldNameOverride;
    return new MonitorUpdateRequestProcessor(
        next, queryFieldName, req.getCore(), presearcher, payloadFieldName);
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    Object presearcherType = args.get("presearcherType");
    presearcher = PresearcherFactory.build(presearcherType);
    queryFieldNameOverride = (String) args.get("queryFieldName");
    payloadFieldNameOverride = (String) args.get("payloadFieldName");
  }
}
