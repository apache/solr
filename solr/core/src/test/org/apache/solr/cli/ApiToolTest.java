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
package org.apache.solr.cli;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

public class ApiToolTest extends SolrTestCase {

  @Test
  public void testParsingGetUrl() throws URISyntaxException {

    String urlWithPercent20SpaceDelimiter =
        "http://localhost:8983/solr/COLL_NAME/sql?stmt=select%20id%20from%20COLL_NAME%20limit%2010";
    String urlWithPlusSpaceDelimiter =
        "http://localhost:8983/solr/COLL_NAME/sql?stmt=select+id+from+COLL_NAME+limit+10";

    for (String url : Arrays.asList(urlWithPercent20SpaceDelimiter, urlWithPlusSpaceDelimiter)) {
      url = url.replace("+", "%20");

      URI uri = new URI(url);
      ModifiableSolrParams params = ApiTool.getSolrParamsFromUri(uri);
      assertEquals(1, params.size());
      assertEquals("select id from COLL_NAME limit 10", params.get("stmt"));
    }
  }
}
