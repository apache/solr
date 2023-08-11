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
package org.apache.solr.util;

import java.io.File;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class RegexFileFilterTest extends SolrTestCase {

  private final RegexFileFilter endsWithDotTxt = new RegexFileFilter(".*\\.txt$");
  private final RegexFileFilter alphaWithTxtOrPdfExt = new RegexFileFilter("^[a-z]+\\.(txt|pdf)$");

  @Test
  public void testAcceptTrue() {
    assertTrue(endsWithDotTxt.accept(new File("/foo/bar/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("~/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("~/1234-abc_.txt")));
    assertTrue(endsWithDotTxt.accept(new File(".txt")));
    assertTrue(alphaWithTxtOrPdfExt.accept(new File("/foo/bar.txt")));
    assertTrue(alphaWithTxtOrPdfExt.accept(new File("/foo/baz.pdf")));
  }

  @Test
  public void testAcceptFalse() {
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.tx")));
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.txts")));
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.exe")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("/foo/bar/b4z.txt")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("/foo/bar/baz.jpg")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("~/foo-bar.txt")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("~/foobar123.txt")));
  }
}
