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

package org.apache.solr.monitor;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.solr.core.SolrCore;

public interface SolrMonitorQueryDecoder {

  MonitorQuery decode(MonitorDataValues monitorDataValues) throws IOException;

  static SolrMonitorQueryDecoder fromCore(SolrCore core) {
    return monitorDataValues -> {
      String id = monitorDataValues.getQueryId();
      String queryStr = monitorDataValues.getMq();
      var query = SimpleQueryParser.parse(queryStr, core);
      String payload = monitorDataValues.getPayload();
      if (payload == null) {
        return new MonitorQuery(id, query, queryStr, Map.of());
      }
      return new MonitorQuery(id, query, queryStr, Map.of(MonitorFields.PAYLOAD, payload));
    };
  }
}
