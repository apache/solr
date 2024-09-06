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
package org.apache.solr.crossdc.handler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      SolrKafkaTestsIgnoredThreadsFilter.class
    })
@ThreadLeakLingering(linger = 5000)
public class MirroringConfigSetsHandlerTest extends SolrTestCaseJ4 {

  private KafkaMirroringSink sink = Mockito.mock(KafkaMirroringSink.class);
  private CoreContainer coreContainer = Mockito.mock(CoreContainer.class);
  private ZkController zkController = Mockito.mock(ZkController.class);
  private SolrZkClient solrZkClient = Mockito.mock(SolrZkClient.class);
  private ArgumentCaptor<MirroredSolrRequest<?>> captor;
  private SolrCore solrCore = Mockito.mock(SolrCore.class);

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  private static SolrQueryRequest createRequest(
      SolrCore solrCore,
      ConfigSetParams.ConfigSetAction action,
      String configSetName,
      String zipResource)
      throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(solrCore, params);
    params.set(ConfigSetParams.ACTION, action.toLower());
    String method = "GET";
    if (action == ConfigSetParams.ConfigSetAction.UPLOAD) {
      method = "POST";
      byte[] content = IOUtils.resourceToByteArray(zipResource);
      params.set(CommonParams.NAME, configSetName);
      List<ContentStream> streams =
          List.of(new ContentStreamBase.ByteArrayStream(content, configSetName, "application/zip"));
      req.setContentStreams(streams);
    }
    req.getContext().put("httpMethod", method);
    return req;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Mockito.when(coreContainer.isZooKeeperAware()).thenReturn(true);
    Mockito.when(coreContainer.getZkController()).thenReturn(zkController);
    Mockito.when(zkController.getZkClient()).thenReturn(solrZkClient);
    Mockito.doAnswer(inv -> null)
        .when(solrZkClient)
        .getData(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    captor = ArgumentCaptor.forClass(MirroredSolrRequest.class);
    Mockito.doNothing().when(sink).submit(captor.capture());
  }

  @Test
  public void testList() throws Exception {
    SolrQueryRequest req =
        createRequest(solrCore, ConfigSetParams.ConfigSetAction.LIST, null, null);
    runCommand(req, false, true);
  }

  @Test
  public void testUpload() throws Exception {
    SolrQueryRequest req =
        createRequest(
            solrCore,
            ConfigSetParams.ConfigSetAction.UPLOAD,
            "testConf",
            "/configs/cloud-minimal.zip");
    runCommand(req, true, true);
  }

  @SuppressWarnings("unchecked")
  private void runCommand(SolrQueryRequest req, boolean expectStreams, boolean expectResult)
      throws Exception {
    int initialMirroredCount = captor.getAllValues().size();
    MirroringConfigSetsHandler handler =
        Mockito.spy(new MirroringConfigSetsHandler(coreContainer, sink));
    Mockito.doNothing().when(handler).baseHandleRequestBody(Mockito.any(), Mockito.any());
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    if (expectResult) {
      assertEquals(
          "should capture additional mirrored value",
          initialMirroredCount + 1,
          captor.getAllValues().size());
      MirroredSolrRequest<?> mirroredSolrRequest = captor.getValue();
      assertNotNull("missing mirrored request", mirroredSolrRequest);
      assertEquals(MirroredSolrRequest.Type.CONFIGSET, mirroredSolrRequest.getType());
      SolrRequest<?> solrRequest = mirroredSolrRequest.getSolrRequest();
      SolrParams mirroredParams = solrRequest.getParams();
      req.getParams()
          .forEach(
              entry -> {
                assertEquals(entry.getValue(), mirroredParams.getParams(entry.getKey()));
              });
      assertEquals("HTTP method", req.getHttpMethod(), solrRequest.getMethod().toString());
      if (expectStreams) {
        List<ContentStream> sourceStreams = (List<ContentStream>) req.getContentStreams();
        assertNotNull("source streams missing", sourceStreams);
        List<ContentStream> mirroredStreams = (List<ContentStream>) solrRequest.getContentStreams();
        assertNotNull("mirrored streams missing", mirroredStreams);
        assertEquals("number of streams", sourceStreams.size(), mirroredStreams.size());
        for (int i = 0; i < sourceStreams.size(); i++) {
          ContentStream source = sourceStreams.get(i);
          ContentStream mirrored = mirroredStreams.get(i);
          assertEquals("name", source.getName(), mirrored.getName());
          assertEquals("contentType", source.getContentType(), mirrored.getContentType());
          byte[] sourceContent =
              MirroredSolrRequest.ExposedByteArrayContentStream.of(source).byteArray();
          byte[] mirroredContent =
              MirroredSolrRequest.ExposedByteArrayContentStream.of(mirrored).byteArray();
          assertTrue("different content", Arrays.equals(sourceContent, mirroredContent));
        }
      }
    } else {
      assertEquals(initialMirroredCount, captor.getAllValues().size());
    }
  }

  @Test
  public void testCoreContainerInit() throws Exception {
    Path home = createTempDir();
    String solrXml = IOUtils.resourceToString("/mirroring-solr.xml", StandardCharsets.UTF_8);
    NodeConfig nodeConfig = SolrXmlConfig.fromString(home, solrXml);
    assertEquals(
        MirroringConfigSetsHandler.class.getName(), nodeConfig.getConfigSetsHandlerClass());
  }
}
