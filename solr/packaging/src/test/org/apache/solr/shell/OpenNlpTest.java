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

package org.apache.solr.shell;

import java.nio.file.Path;
import java.util.List;
import org.junit.Test;

/**
 * Port of {@code test_opennlp.bats}.
 *
 * <p>The two model downloads land in the session's work directory rather than in {@code
 * ${SOLR_TIP}/models/sentiment/}: the distribution is shared by every class running concurrently
 * and must not be written to.
 */
public class OpenNlpTest extends SolrShellTestCase {

  private static final String MODEL_URL =
      "https://nightlies.apache.org/solr/opennlp-dl-integration/model_quantized.onnx";
  private static final String VOCAB_URL =
      "https://nightlies.apache.org/solr/opennlp-dl-integration/vocab.txt";

  // This BATS style test is really just to help explore the space of Modern NLP in
  // Apache Solr, versus a "true" integration test that I want to have run regularly.
  // On the other hand, since integrating NLP requires a lot of steps, maybe having this
  // long test as an "integration" test is something we decide is okay?
  // I also have dreams of incorporating this as code snippets in a Tutorial via the ascii doc tags
  // like we use for the SolrJ code snippets.  That way we know the snippets continue to work!
  @Test
  // Nightly: it downloads a 160MB ONNX sentiment model and its
  // vocabulary from nightlies.apache.org, which an ordinary build has no business doing.
  @Nightly
  public void checkLifecycleOfSentimentClassification() {
    // Downloading onnx model and vocab...
    // Models originally sourced from Huggingface and stored on ASF hardware.
    Path model = download(MODEL_URL, "models/sentiment/model.onnx");
    Path vocab = download(VOCAB_URL, "models/sentiment/vocab.txt");

    List<Path> moduleLibs =
        list(harnessEnv.solrTip().resolve("modules/analysis-extras/lib").toString());
    assertFalse("No jars in the analysis-extras module", moduleLibs.isEmpty());
    assertFalse(
        "The module must ship the CPU build of onnxruntime: " + moduleLibs,
        moduleLibs.toString().contains("onnxruntime_gpu"));

    // Can't figure out magic policy stuff to allow loading ONNX, so disable security manager.
    env("SOLR_SECURITY_MANAGER_ENABLED", "false");

    run("solr start -m 4g -Dsolr.modules=analysis-extras -Dsolr.packages.enabled=true");
    run("solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000");

    run("solr create -c COLL_NAME").assertContains("Created collection 'COLL_NAME'");

    httpPost(
            "/solr/COLL_NAME/schema",
            "application/json",
            """
            {
              "add-field":{
                "name":"name",
                "type":"string",
                "stored":true }
            }
            """)
        .assertStatus(200)
        .assertContains("\"status\":0");

    httpPost(
            "/solr/COLL_NAME/schema",
            "application/json",
            """
            {
              "add-field":{
                "name":"name_sentiment",
                "type":"string",
                "stored":true }
            }
            """)
        .assertStatus(200)
        .assertContains("\"status\":0");

    // curl --data-binary @file -X PUT. The endpoint consumes the raw request body, so the type is
    // named explicitly rather than left to curl's form-encoded default.
    httpPut(
            "/api/cluster/filestore/files/models/sentiment/vocab.txt",
            "application/octet-stream",
            vocab)
        .assertContains("\"status\":0");

    httpPut(
            "/api/cluster/filestore/files/models/sentiment/model.onnx",
            "application/octet-stream",
            model)
        .assertContains("\"status\":0");

    httpPost(
            "/solr/COLL_NAME/config",
            "application/json",
            """
            {
              "add-updateprocessor": {
                "name": "sentimentClassifier",
                "class": "solr.processor.DocumentCategorizerUpdateProcessorFactory",
                "modelFile": "models/sentiment/model.onnx",
                "vocabFile": "models/sentiment/vocab.txt",
                "source": "name",
                "dest": "name_sentiment"
              }
            }
            """)
        .assertContains("\"status\":0");

    httpPost(
            "/solr/COLL_NAME/update/json?processor=sentimentClassifier&commit=true",
            "application/json",
            """
            [
              {
                "id":"good",
                "name" : "that was an awesome movie!"
              },
              {
                "id":"bad",
                "name" : "that movie was bad and terrible"
              }
            ]
            """)
        .assertContains("\"status\":0");

    http("/solr/COLL_NAME/select?q=id:good").assertContains("\"name_sentiment\":\"very good\"");
    http("/solr/COLL_NAME/select?q=id:bad").assertContains("\"name_sentiment\":\"very bad\"");
  }
}
