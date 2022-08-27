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
package org.apache.solr.core;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class QuerySenderListenerTest extends SolrTestCaseJ4 {

  @Test
  public void testConvertQueriesToList() {

    // represents a warming query
    NamedList<Object> query = new NamedList<Object>();

    // in the JSON config model, queries is an ArrayList of NamedLists
    ArrayList<Object> queries = new ArrayList<Object>();
    queries.add(query);

    // in the XML config model, queries is an ArrayList of ArrayLists of NamedLists
    ArrayList<Object> queriesList = new ArrayList<Object>();
    queriesList.add(query);
    queries.add(queriesList);

    // this unexpected item should be ignored
    queries.add("test");

    // the output having a length of 2 proves both expected models were handled
    List<NamedList<Object>> allLists = QuerySenderListener.convertQueriesToList(queries);
    assertEquals(allLists.size(), 2);
  }
}
