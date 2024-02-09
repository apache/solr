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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.util.DOMConfigNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/** Test for {@link CacheConfig}*/
public class CacheConfigTest extends SolrTestCaseJ4 {
  private static final String XPATH_DOCUMENT_CACHE = "query/documentCache";
  private static final String XPATH_QUERY_RESULT_CACHE = "query/queryResultCache";
  private SolrResourceLoader mockSolrResourceLoader;
  private ConfigOverlay mockConfigOverlay;
  private ConfigNode overlayConfigNode;
  private ConfigNode domConfigNode;
  private ConfigNode domConfigNodeDisable;
  private SolrConfig mockSolrConfig;
  private static final DocumentBuilder docBuilder;

  static {
    try {
      docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @BeforeClass
  public static void ensureWorkingMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    resetMocks();
    final String initialSize = "99";
    final String size = "999";

    final Document doc = docBuilder.newDocument();
    final Element documentCacheNode = doc.createElement("documentCache");
    documentCacheNode.setAttribute("initialSize", initialSize);
    documentCacheNode.setAttribute("size", size);
    documentCacheNode.setAttribute("enabled", "true");
    domConfigNode = new DOMConfigNode(documentCacheNode);
    Mockito.when(mockSolrConfig.getOverlay()).thenReturn(mockConfigOverlay);
    Mockito.when(mockConfigOverlay.getEditableSubProperties(XPATH_DOCUMENT_CACHE)).thenReturn(null);

    final String sizeForQueryResCache = "199";
    final Document queryResultCacheDoc = docBuilder.newDocument();
    final Element queryResultNode = queryResultCacheDoc.createElement("queryResultCache");
    queryResultNode.setAttribute("initialSize", "99");
    queryResultNode.setAttribute("size", sizeForQueryResCache);
    queryResultNode.setAttribute("enabled", "false");
    domConfigNodeDisable = new DOMConfigNode(queryResultNode);
  }

  public void testSolrConfigPropsForCache() {
    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, domConfigNode, XPATH_DOCUMENT_CACHE);
    assertNotNull(cacheConfig);
    final Map<String, Object> args = cacheConfig.toMap(new HashMap<>());
    assertNotNull(args);
    assertEquals("99", args.get("initialSize"));
    assertEquals("999", args.get("size"));
    assertEquals("documentCache", cacheConfig.getNodeName());
  }

  public void testSolrConfigDisabledCache() {
    Mockito.when(mockSolrConfig.getOverlay()).thenReturn(mockConfigOverlay);
    Mockito.when(mockConfigOverlay.getEditableSubProperties(XPATH_QUERY_RESULT_CACHE))
        .thenReturn(null);

    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, domConfigNodeDisable, XPATH_QUERY_RESULT_CACHE);
    assertNull(cacheConfig);
  }

  public void testOverlayPropsOverridingSolrConfigProps() {
    final String overlaidSize = "199";
    overlayConfigNode =
        new OverlaidConfigNode(mockConfigOverlay, "documentCache", null, domConfigNode);
    Mockito.when(mockSolrConfig.getOverlay()).thenReturn(mockConfigOverlay);
    Mockito.when(mockConfigOverlay.getEditableSubProperties(XPATH_DOCUMENT_CACHE))
        .thenReturn(Map.of("size", overlaidSize));

    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, domConfigNode, XPATH_DOCUMENT_CACHE);
    assertNotNull(cacheConfig);
    final Map<String, Object> args = cacheConfig.toMap(new HashMap<>());
    assertNotNull(args);
    assertEquals(overlaidSize, args.get("size"));
  }

  public void testOverlaidDisabledSolrConfigEnabledCache() {
    overlayConfigNode =
        new OverlaidConfigNode(mockConfigOverlay, "queryResultCache", null, domConfigNode);
    Mockito.when(mockSolrConfig.getOverlay()).thenReturn(mockConfigOverlay);
    Mockito.when(mockConfigOverlay.getXPathProperty(Arrays.asList("queryResultCache", "enabled")))
        .thenReturn("false");

    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, overlayConfigNode, XPATH_QUERY_RESULT_CACHE);
    assertNull(cacheConfig);
  }

  public void testOverlaidEnabledSolrConfigDisabledCache() {
    overlayConfigNode =
        new OverlaidConfigNode(mockConfigOverlay, "queryResultCache", null, domConfigNodeDisable);
    Mockito.when(mockSolrConfig.getOverlay()).thenReturn(mockConfigOverlay);
    Mockito.when(mockConfigOverlay.getXPathProperty(Arrays.asList("queryResultCache", "enabled")))
        .thenReturn("true");

    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, overlayConfigNode, XPATH_QUERY_RESULT_CACHE);
    assertNotNull(cacheConfig);
    final Map<String, Object> args = cacheConfig.toMap(new HashMap<>());
    assertNotNull(args);
    assertEquals("99", args.get("initialSize"));
    assertEquals("queryResultCache", cacheConfig.getNodeName());
  }

  public void testEmptyConfigNodeCache() {
    final ConfigNode emptyNodeSolrConfig = ConfigNode.EMPTY;
    final CacheConfig cacheConfig =
        CacheConfig.getConfig(mockSolrConfig, emptyNodeSolrConfig, null);
    assertNull(cacheConfig);
  }

  private void resetMocks() {
    mockSolrConfig = mock(SolrConfig.class);
    mockConfigOverlay = mock(ConfigOverlay.class);
    mockSolrResourceLoader = mock(SolrResourceLoader.class);
    when(mockSolrConfig.getResourceLoader()).thenReturn(mockSolrResourceLoader);
  }
}
