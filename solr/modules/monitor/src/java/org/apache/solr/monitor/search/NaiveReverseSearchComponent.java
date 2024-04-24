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

package org.apache.solr.monitor.search;

import java.io.IOException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.search.QParserPlugin;

public class NaiveReverseSearchComponent extends SearchComponent {

  @Override
  public void prepare(ResponseBuilder rb) {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    QParserPlugin plugin = rb.req.getCore().getQueryPlugin(NaiveReverseQueryParserPlugin.NAME);
    if (plugin instanceof NaiveReverseQueryParserPlugin) {
      NaiveReverseQueryParserPlugin rqpp = (NaiveReverseQueryParserPlugin) plugin;
      rqpp.process(rb.req, rb.rsp);
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    // TODO
    return super.distributedProcess(rb);
  }

  @Override
  public String getDescription() {
    return "Component that integrates with lucene monitor for reverse search.";
  }
}
