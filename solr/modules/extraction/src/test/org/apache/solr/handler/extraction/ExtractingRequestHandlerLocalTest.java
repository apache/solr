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

import org.junit.Test;

public class ExtractingRequestHandlerLocalTest extends ExtractingRequestHandlerTestAbstract {
  @Test
  public void testPdfWithImages() throws Exception {
    // This test moved from abstract class since TikaServer with Tika3 does not extract images by
    // default
    // Tests possibility to configure ParseContext (by example to extract embedded images from pdf)
    loadLocal(
        "extraction/pdf-with-image.pdf",
        "fmap.created",
        "extractedDate",
        "fmap.producer",
        "extractedProducer",
        "fmap.creator",
        "extractedCreator",
        "fmap.Keywords",
        "extractedKeywords",
        "fmap.Creation-Date",
        "extractedDate",
        "uprefix",
        "ignored_",
        "fmap.Author",
        "extractedAuthor",
        "fmap.content",
        "wdf_nocase",
        "literal.id",
        "pdfWithImage",
        "resource.name",
        "pdf-with-image.pdf",
        "resource.password",
        "solrRules",
        "fmap.Last-Modified",
        "extractedDate");

    assertQ(req("wdf_nocase:\"embedded:image0.jpg\""), "//*[@numFound='0']");
    assertU(commit());
    assertQ(req("wdf_nocase:\"embedded:image0.jpg\""), "//*[@numFound='1']");
  }
}
