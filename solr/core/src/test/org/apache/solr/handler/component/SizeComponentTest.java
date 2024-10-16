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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SizeParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressSSL
public class SizeComponentTest extends SolrTestCaseJ4 {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initCore("solrconfig-sizing.xml", "schema11.xml");
    assertU(
        adoc("id", "1", "text", "Add one document with some text to fake an index.", "str_s", "a"));
    assertU(
        adoc(
            "id",
            "2",
            "text",
            "Add another document with some text to fake a barely bigger index.",
            "str_s",
            "b"));
    assertU(
        adoc(
            "id",
            "3",
            "text",
            "Add one more document with some text to fake a barely bigger index.",
            "str_s",
            "c"));
    assertU(
        adoc("id", "4", "text", "Then add another document with some more text.", "str_s", "d"));
    assertU(
        adoc(
            "id",
            "5",
            "text",
            "And then add yet another document with a little bit more text.",
            "str_s",
            "e"));
    assertU(adoc("id", "6", "text", "And, well, let's stop right here.", "str_s", "f"));
    assertU(commit());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    deleteCore();
  }

  @Test
  public void testDefaultOutput() throws Exception {

    assertQ(
        req(
            CommonParams.Q,
            "*:*",
            CommonParams.WT,
            "xml",
            CommonParams.ROWS,
            "0",
            SizeParams.SIZE,
            "true"),
        "//*[@numFound='6']",
        "//lst[@name='size']/str[@name='totalDiskSize'][contains(text(), 'KB')]",
        "//lst[@name='size']/str[@name='totalLuceneRam'][contains(text(), 'MB')]",
        "//lst[@name='size']/str[@name='totalSolrRam'][contains(text(), 'MB')]",
        "//lst[@name='size']/long[@name='estimatedNumDocs'][.='6']",
        "//lst[@name='size']/str[@name='estimatedDocSize'][contains(text(), 'bytes')]",
        "//lst[@name='size']/lst[@name='solrDetails']/str[@name='filterCache'][contains(text(), 'KB')]",
        "//lst[@name='size']/lst[@name='solrDetails']/str[@name='queryResultCache'][contains(text(), 'KB')]",
        "//lst[@name='size']/lst[@name='solrDetails']/str[@name='documentCache'][contains(text(), 'KB')]",
        "//lst[@name='size']/lst[@name='solrDetails']/str[@name='luceneRam'][contains(text(), 'MB')]");
  }

  @Test
  public void testNormalizedGB() throws Exception {

    assertQ(
        req(
            CommonParams.Q,
            "*:*",
            CommonParams.WT,
            "xml",
            CommonParams.ROWS,
            "0",
            SizeParams.SIZE,
            "true",
            SizeParams.SIZE_UNIT,
            "GB"),
        "//*[@numFound='6']",
        "//lst[@name='size']/double[@name='totalDiskSize']",
        "//lst[@name='size']/double[@name='totalLuceneRam'][.>=0.01][.<10.0]",
        "//lst[@name='size']/double[@name='totalSolrRam'][.>=0.01][.<10.0]",
        "//lst[@name='size']/long[@name='estimatedNumDocs'][.='6']",
        "//lst[@name='size']/double[@name='estimatedDocSize']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='filterCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='queryResultCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='documentCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='luceneRam'][.>=0.01][.<10.0]");
  }

  @Test
  public void testNormalizedMB() throws Exception {

    assertQ(
        req(
            CommonParams.Q,
            "*:*",
            CommonParams.WT,
            "xml",
            CommonParams.ROWS,
            "0",
            SizeParams.SIZE,
            "true",
            SizeParams.SIZE_UNIT,
            "MB"),
        "//*[@numFound='6']",
        "//lst[@name='size']/double[@name='totalDiskSize']",
        "//lst[@name='size']/double[@name='totalLuceneRam'][.>=10.0][.<1000.0]",
        "//lst[@name='size']/double[@name='totalSolrRam'][.>=10.0][.<1000.0]",
        "//lst[@name='size']/long[@name='estimatedNumDocs'][.='6']",
        "//lst[@name='size']/double[@name='estimatedDocSize']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='filterCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='queryResultCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='documentCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='luceneRam'][.>=10.0][.<1000.0]");
  }

  @Test
  public void testNormalizedKB() throws Exception {

    assertQ(
        req(
            CommonParams.Q,
            "*:*",
            CommonParams.WT,
            "xml",
            CommonParams.ROWS,
            "0",
            SizeParams.SIZE,
            "true",
            SizeParams.SIZE_UNIT,
            "KB"),
        "//*[@numFound='6']",
        "//lst[@name='size']/double[@name='totalDiskSize']",
        "//lst[@name='size']/double[@name='totalLuceneRam'][.>=1000.0]",
        "//lst[@name='size']/double[@name='totalSolrRam'][.>=1000.0]",
        "//lst[@name='size']/long[@name='estimatedNumDocs'][.='6']",
        "//lst[@name='size']/double[@name='estimatedDocSize']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='filterCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='queryResultCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='documentCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='luceneRam'][.>=1000.0]");
  }

  @Test
  public void testNormalizedBytes() throws Exception {

    assertQ(
        req(
            CommonParams.Q,
            "*:*",
            CommonParams.WT,
            "xml",
            CommonParams.ROWS,
            "0",
            SizeParams.SIZE,
            "true",
            SizeParams.SIZE_UNIT,
            "bytes"),
        "//*[@numFound='6']",
        "//lst[@name='size']/double[@name='totalDiskSize'][.>=1000.0]",
        "//lst[@name='size']/double[@name='totalLuceneRam'][contains(string(.),'E')]",
        "//lst[@name='size']/double[@name='totalSolrRam'][contains(string(.),'E')]",
        "//lst[@name='size']/long[@name='estimatedNumDocs'][.='6']",
        "//lst[@name='size']/double[@name='estimatedDocSize']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='filterCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='queryResultCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='documentCache']",
        "//lst[@name='size']/lst[@name='solrDetails']/double[@name='luceneRam'][contains(string(.),'E')]");
  }
}
