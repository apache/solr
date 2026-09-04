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
package org.apache.solr.client.solrj.io.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/** Tests the connection string part of the JDBC Driver */
public class JdbcDriverTest extends SolrTestCaseJ4 {

  @Test(expected = SQLException.class)
  public void testNullZKConnectionString() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://?collection=collection1");
  }

  @Test(expected = SQLException.class)
  public void testInvalidJDBCConnectionString() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:mysql://");
  }

  @Test(expected = SQLException.class)
  public void testNoCollectionProvidedInURL() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://?collection=collection1");
  }

  @Test(expected = SQLException.class)
  public void testNoCollectionProvidedInProperties() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://", new Properties());
  }

  @Test(expected = SQLException.class)
  public void testConnectionStringJumbled() throws Exception {
    final String sampleZkHost = "zoo1:9983/foo";
    DriverManager.getConnection(
        "solr:jdbc://" + sampleZkHost + "?collection=collection1", new Properties());
  }
}
