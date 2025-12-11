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
package org.apache.solr.client.ref_guide_examples;

import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.clear;
import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.ensureNoLeftoverOutputExpectations;
import static org.apache.solr.client.ref_guide_examples.ExpectedOutputVerifier.print;

import java.nio.file.Path;
import java.util.List;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage for Solr Cell (Apache Tika) PDF extraction.
 *
 * <p>Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference
 * Guide.
 */
public class SolrCellRefGuideExamplesTest extends SolrCloudTestCase {
  private static final String PDF_DOC_PATH =
      ExternalPaths.TECHPRODUCTS_CONFIGSET + "/../../../../../example/exampledocs/solr-word.pdf";
  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "extraction_config";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(CONFIG_NAME, ExternalPaths.TECHPRODUCTS_CONFIGSET).configure();

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
        .process(cluster.getSolrClient());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    clear();
  }

  @Test
  public void testSolrCellPdfExtraction() throws Exception {
    // expectLine("Update status was: 0");

    // Best we can do without including extraction module
    assertExceptionThrownWithMessageContaining(
        SolrException.class,
        List.of("Error loading class 'solr.extraction.ExtractingRequestHandler'"),
        () -> {
          // tag::solr-cell-pdf-extraction[]
          ContentStreamUpdateRequest extractRequest =
              new ContentStreamUpdateRequest("/update/extract");
          extractRequest.addFile(Path.of(PDF_DOC_PATH), "application/pdf");
          extractRequest.setParam("extractOnly", "true");
          NamedList<Object> extractResponse =
              cluster.getSolrClient().request(extractRequest, COLLECTION_NAME);
          print("Update status was: " + extractResponse.getName(0));
          // end::solr-cell-pdf-extraction[]
        });
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    ensureNoLeftoverOutputExpectations();
  }
}
