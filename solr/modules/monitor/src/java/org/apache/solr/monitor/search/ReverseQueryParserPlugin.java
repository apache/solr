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
import org.apache.lucene.monitor.Presearcher;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.monitor.PresearcherFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class ReverseQueryParserPlugin extends QParserPlugin {

  public static final String NAME = "reverse";

  // TODO parameterize this
  private Presearcher presearcher = PresearcherFactory.build(PresearcherFactory.TERM_FILTERED);

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ReverseQueryParser(qstr, localParams, params, req, presearcher);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
  }
}
