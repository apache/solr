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
package org.apache.solr.llm;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.llm.texttovector.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.util.RestTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLlmBase extends RestTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static Path tmpSolrHome;
  protected static Path tmpConfDir;

  public static final String MODEL_FILE_NAME = "_schema_text-to-vector-model-store.json";
  protected static final String COLLECTION = "collection1";
  protected static final String CONF_DIR = COLLECTION + "/conf";

  protected static Path embeddingModelStoreFile = null;

  protected static String IDField = "id";
  protected static String vectorField = "vector";
  protected static String vectorField2 = "vector2";
  protected static String vectorFieldByteEncoding = "vector_byte_encoding";

  protected static void setupTest(
      String solrconfig, String schema, boolean buildIndex, boolean persistModelStore)
      throws Exception {
    initFolders(persistModelStore);
    createJettyAndHarness(
        tmpSolrHome.toAbsolutePath().toString(), solrconfig, schema, "/solr", true, null);
    if (buildIndex) prepareIndex();
  }

  protected static void initFolders(boolean isPersistent) throws Exception {
    tmpSolrHome = createTempDir();
    tmpConfDir = tmpSolrHome.resolve(CONF_DIR);
    tmpConfDir.toFile().deleteOnExit();
    PathUtils.copyDirectory(TEST_PATH(), tmpSolrHome.toAbsolutePath());

    final Path modelStore = tmpConfDir.resolve(MODEL_FILE_NAME);

    if (isPersistent) {
      embeddingModelStoreFile = modelStore;
    }

    if (Files.exists(modelStore)) {
      if (log.isInfoEnabled()) {
        log.info("remove model store config file in {}", modelStore.toAbsolutePath());
      }
      Files.delete(modelStore);
    }

    System.setProperty("managed.schema.mutable", "true");
  }

  protected static void afterTest() throws Exception {
    if (null != restTestHarness) {
      restTestHarness.close();
      restTestHarness = null;
    }
    solrClientTestRule.reset();
    if (null != tmpSolrHome) {
      PathUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    System.clearProperty("managed.schema.mutable");
  }

  public static void loadModel(String fileName, String status) throws Exception {
    final URL url = TestLlmBase.class.getResource("/modelExamples/" + fileName);
    final String multipleModels = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    assertJPut(
        ManagedTextToVectorModelStore.REST_END_POINT,
        multipleModels,
        "/responseHeader/status==" + status);
  }

  public static void loadModel(String fileName) throws Exception {
    final URL url = TestLlmBase.class.getResource("/modelExamples/" + fileName);
    final String multipleModels = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    assertJPut(
        ManagedTextToVectorModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");
  }

  protected static void prepareIndex() throws Exception {
    List<SolrInputDocument> docsToIndex = prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }

    assertU(commit());
  }

  private static List<SolrInputDocument> prepareDocs() {
    int docsCount = 13;
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);
    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    docs.get(0)
        .addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector1= 1.0
    docs.get(1)
        .addField(
            vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector1= 0.998
    docs.get(2)
        .addField(
            vectorField,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector1= 0.992
    docs.get(3)
        .addField(
            vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f)); // cosine distance vector1= 0.999
    docs.get(4)
        .addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f)); // cosine distance vector1= 0.862
    docs.get(5)
        .addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f)); // cosine distance vector1= 0.756
    docs.get(6)
        .addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f)); // cosine distance vector1= 0.970
    docs.get(7)
        .addField(
            vectorField, Arrays.asList(120f, 60f, 30f, 15f)); // cosine distance vector1= 0.515
    docs.get(8)
        .addField(
            vectorField, Arrays.asList(200f, 50f, 100f, 25f)); // cosine distance vector1= 0.554
    docs.get(9)
        .addField(
            vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f)); // cosine distance vector1= 0.997
    docs.get(10)
        .addField(vectorField2, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector2= 1
    docs.get(11)
        .addField(
            vectorField2,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector2= 0.992
    docs.get(12)
        .addField(
            vectorField2, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector2= 0.998

    docs.get(0).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 3, 4));
    docs.get(1).addField(vectorFieldByteEncoding, Arrays.asList(2, 2, 1, 4));
    docs.get(2).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 1, 2));
    docs.get(3).addField(vectorFieldByteEncoding, Arrays.asList(7, 2, 1, 3));
    docs.get(4).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(5).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(6).addField(vectorFieldByteEncoding, Arrays.asList(18, 2, 4, 4));
    docs.get(7).addField(vectorFieldByteEncoding, Arrays.asList(8, 3, 2, 4));

    return docs;
  }
}
