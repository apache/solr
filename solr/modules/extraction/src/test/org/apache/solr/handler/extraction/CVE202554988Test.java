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
package org.apache.solr.handler.extraction;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for CVE-2025-54988 mitigation.
 *
 * <p>This test verifies that the default Tika configuration in Solr prevents XXE (XML External
 * Entity) injection attacks via crafted XFA (XML Forms Architecture) content in PDF files.
 *
 * <p>CVE-2025-54988 affects Apache Tika versions 1.13 through 3.2.1, allowing attackers to exploit
 * XXE vulnerabilities in PDF parsing when XFA forms are processed.
 *
 * <p>The mitigation disables XFA parsing by setting: - extractAcroFormContent = false -
 * ifXFAExtractOnlyXFA = false
 */
public class CVE202554988Test extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(
        "solrconfig.xml", "schema.xml", getFile("extraction/solr/collection1").getParent());
  }

  /**
   * Test that the default Tika configuration prevents XFA parsing and thus mitigates CVE-2025-54988.
   *
   * <p>The default configuration should NOT extract XFA content, preventing XXE attacks.
   */
  @Test
  public void testDefaultConfigPreventsXFAParsing() throws Exception {
    // Load the PDF with XFA/XXE content using hardened default config
    loadLocal("extraction/pdf-with-xfa-xxe.pdf",
        "literal.id", "doc-default",
        "fmap.content", "extractedContent",
        "uprefix", "ignored_");

    assertU(commit());

    // Verify document was indexed
    assertQ(req("id:doc-default"), "//*[@numFound='1']");

    // Verify the document has extractedContent field
    assertQ(
        req("id:doc-default"),
        "//arr[@name='extractedContent']/str"
    );

    // Verify that XFA form field names are NOT extracted (proves XFA parsing is disabled)
    // The PDF contains XFA fields named "companyName", "secretData", and "xxeField"
    // These field names should NOT appear when XFA parsing is disabled
    assertQ(
        req("extractedContent:companyName"),
        "//*[@numFound='0']"
    );

    assertQ(
        req("extractedContent:secretData"),
        "//*[@numFound='0']"
    );

    assertQ(
        req("extractedContent:xxeField"),
        "//*[@numFound='0']"
    );
  }
  
  /**
   * Test with the original vulnerable Tika configuration (pre-9.10.0) to verify document extraction.
   *
   * <p>This test uses the original solr-default-tika-config.xml that was used up to Solr 9.10.0,
   * which lacked explicit XFA parsing protections and was therefore vulnerable to CVE-2025-54988.
   *
   * <p>While Tika 1.28.5 may not extract XFA content by default in all cases, the lack of
   * explicit protection parameters means the configuration was vulnerable to XXE attacks when
   * XFA content is processed. The new hardened config explicitly disables XFA parsing.
   *
   * <p>This test verifies that:
   * 1. PDFs can still be extracted with the old config
   * 2. The document content (non-XFA) is extracted normally
   * 3. The old config lacks the explicit XFA protections that the new config has
   */
  @Test
  public void testVulnerableConfigLacksXFAProtections() throws Exception {
    File tikaConfig = getFile("extraction/tika-config-vulnerable.xml");

    // Load with the original vulnerable config (used up to Solr 9.10.0)
    loadLocal("extraction/pdf-with-xfa-xxe.pdf",
        "literal.id", "doc-vulnerable",
        "tika.config", tikaConfig.getAbsolutePath(),
        "fmap.content", "extractedContent",
        "uprefix", "ignored_");

    assertU(commit());

    // Verify document was indexed successfully
    assertQ(req("id:doc-vulnerable"), "//*[@numFound='1']");

    // Verify basic PDF content is extracted (the non-XFA text "Test PDF")
    assertQ(
        req("extractedContent:Test AND extractedContent:PDF"),
        "//*[@numFound='1']"
    );

    // CRITICAL: With the vulnerable config, XFA field names ARE extracted
    // This proves XFA content is being parsed - the attack vector for CVE-2025-54988
    assertQ(
        req("extractedContent:companyName"),
        "//*[@numFound='1']"
    );

    assertQ(
        req("extractedContent:secretData"),
        "//*[@numFound='1']"
    );

    assertQ(
        req("extractedContent:xxeField"),
        "//*[@numFound='1']"
    );
  }

  /**
   * Helper method to load a file into the extraction handler.
   */
  private SolrQueryResponse loadLocal(String filename, String... args) throws Exception {
    LocalSolrQueryRequest req = (LocalSolrQueryRequest) req(args);
    try {
      // Create content stream from test file
      List<ContentStream> cs = new ArrayList<>();
      cs.add(new ContentStreamBase.FileStream(getFile(filename)));
      req.setContentStreams(cs);

      // Get handler and process request
      ExtractingRequestHandler handler =
          (ExtractingRequestHandler) h.getCore().getRequestHandler("/update/extract");
      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(req, rsp);

      return rsp;
    } finally {
      req.close();
    }
  }
}
