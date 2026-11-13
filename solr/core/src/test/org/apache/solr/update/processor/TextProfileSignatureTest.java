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
package org.apache.solr.update.processor;

import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.Test;

public class TextProfileSignatureTest extends SolrTestCase {

  private static TextProfileSignature newSignature() {
    TextProfileSignature sig = new TextProfileSignature();
    sig.init(new MapSolrParams(Map.of()));
    return sig;
  }

  private static byte[] signatureOf(String content) {
    TextProfileSignature sig = newSignature();
    sig.add(content);
    return sig.getSignature();
  }

  private static String hex(byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }

  @Test
  public void testIdenticalContentSameSignature() {
    String content = "the quick brown fox jumps over the lazy dog";
    assertArrayEquals(signatureOf(content), signatureOf(content));
  }

  @Test
  public void testSignatureStableAcrossHashMapResize() {
    String[] head = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"};
    StringBuilder base = new StringBuilder();
    for (String token : head) {
      for (int i = 0; i < 200; i++) {
        if (!base.isEmpty()) {
          base.append(' ');
        }
        base.append(token);
      }
    }

    byte[] expected = signatureOf(base.toString());
    for (int n = 0; n < 80; n++) {
      StringBuilder text = new StringBuilder(base);
      for (int i = 0; i <= n; i++) {
        text.append(' ').append("xpad").append(String.format(Locale.ROOT, "%04d", i));
      }
      byte[] actual = signatureOf(text.toString());
      assertArrayEquals(
          "signature changed after adding "
              + (n + 1)
              + " filler(s); expected "
              + hex(expected)
              + " but was "
              + hex(actual),
          expected,
          actual);
    }
  }

  @Test
  public void testTokenOrderDoesNotAffectSignature() {
    String a = "red red blue blue green green";
    String b = "green green blue blue red red";
    String c = "blue blue red red green green";
    assertArrayEquals(signatureOf(a), signatureOf(b));
    assertArrayEquals(signatureOf(a), signatureOf(c));
  }
}
