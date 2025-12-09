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
    initCore("solrconfig.xml", "schema.xml", getFile("extraction/solr/collection1").getParent());
  }

  /**
   * Test that the default Tika configuration prevents XFA parsing and thus mitigates
   * CVE-2025-54988.
   *
   * <p>The default configuration should NOT extract XFA content, preventing XXE attacks.
   */
  @Test
  public void testDefaultConfigPreventsXFAParsing() throws Exception {
    // Load the PDF with XFA/XXE content using hardened default config
    loadLocal(
        "extraction/pdf-with-xfa-xxe.pdf",
        "literal.id",
        "doc-default",
        "fmap.content",
        "extractedContent",
        "uprefix",
        "ignored_");

    assertU(commit());

    // Verify document was indexed
    assertQ(req("id:doc-default"), "//*[@numFound='1']");

    // Verify the document has extractedContent field
    assertQ(req("id:doc-default"), "//arr[@name='extractedContent']/str");

    // Verify that XFA form field names are NOT extracted (proves XFA parsing is disabled)
    // The PDF contains XFA fields named "companyName", "secretData", and "xxeField"
    // These field names should NOT appear when XFA parsing is disabled
    assertQ(req("id:doc-default AND extractedContent:companyName"), "//*[@numFound='0']");

    assertQ(req("id:doc-default AND extractedContent:secretData"), "//*[@numFound='0']");

    assertQ(req("id:doc-default AND extractedContent:xxeField"), "//*[@numFound='0']");
  }

  /**
   * Test with vulnerable parseContext configuration to demonstrate CVE-2025-54988.
   *
   * <p>This test uses a parseContext configuration that explicitly enables XFA parsing
   * (extractAcroFormContent=true), which was the vulnerable default before the fix.
   *
   * <p>This test verifies that: 1. PDFs can still be extracted when XFA parsing is enabled 2. XFA
   * form field names ARE extracted when extractAcroFormContent=true 3. This demonstrates the attack
   * vector for CVE-2025-54988
   */
  @Test
  public void testVulnerableConfigEnablesXFAParsing() throws Exception {
    // Load using the /update/extract/vulnerable handler which has extractAcroFormContent=true
    loadLocalFromHandler(
        "/update/extract/vulnerable",
        "extraction/pdf-with-xfa-xxe.pdf",
        "literal.id",
        "doc-vulnerable",
        "fmap.content",
        "extractedContent",
        "uprefix",
        "ignored_");

    assertU(commit());

    // Verify document was indexed successfully
    assertQ(req("id:doc-vulnerable"), "//*[@numFound='1']");

    // Verify basic PDF content is extracted (the non-XFA text "Test PDF")
    assertQ(
        req("id:doc-vulnerable AND extractedContent:Test AND extractedContent:PDF"),
        "//*[@numFound='1']");

    // CRITICAL: With extractAcroFormContent=true, XFA field names ARE extracted
    // This proves XFA content is being parsed - the attack vector for CVE-2025-54988
    assertQ(req("id:doc-vulnerable AND extractedContent:companyName"), "//*[@numFound='1']");

    assertQ(req("id:doc-vulnerable AND extractedContent:secretData"), "//*[@numFound='1']");

    assertQ(req("id:doc-vulnerable AND extractedContent:xxeField"), "//*[@numFound='1']");
  }

  /** Helper method to load a file into the default extraction handler. */
  private SolrQueryResponse loadLocal(String filename, String... args) throws Exception {
    return loadLocalFromHandler("/update/extract", filename, args);
  }

  /** Helper method to load a file into a specific extraction handler. */
  private SolrQueryResponse loadLocalFromHandler(String handler, String filename, String... args)
      throws Exception {
    LocalSolrQueryRequest req = (LocalSolrQueryRequest) req(args);
    try {
      // Create content stream from test file
      List<ContentStream> cs = new ArrayList<>();
      cs.add(new ContentStreamBase.FileStream(getFile(filename)));
      req.setContentStreams(cs);

      // Get handler and process request
      ExtractingRequestHandler extractHandler =
          (ExtractingRequestHandler) h.getCore().getRequestHandler(handler);
      SolrQueryResponse rsp = new SolrQueryResponse();
      extractHandler.handleRequest(req, rsp);

      return rsp;
    } finally {
      req.close();
    }
  }
}
