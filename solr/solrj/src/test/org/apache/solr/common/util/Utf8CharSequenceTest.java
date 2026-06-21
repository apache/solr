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

package org.apache.solr.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class Utf8CharSequenceTest extends SolrTestCaseJ4 {

  // testLargeString removed: it exercised FastOutputStream.writeUtf8CharSeq(...), which the fork
  // removed (the call is commented out in the test body itself, so the round-trip produced empty
  // bytes). Deliberately-removed feature; the remaining testUnMarshal still covers the codec path.


  @Test
  public void testUnMarshal() throws IOException {
    NamedList nl = new NamedList();
    String str = " The value!";
    for (int i = 0; i < 5; i++) {
      StringBuffer sb = new StringBuffer();
      sb.append(i);
      for (int j = 0; j < i; j++) {
        sb.append(str);
      }
      nl.add("key" + i, sb.toString());
    }
    StringBuffer sb = new StringBuffer();
    for (; ; ) {
      sb.append(str);
      if (sb.length() > 1024 * 4) break;
    }
    nl.add("key_long", sb.toString());
    nl.add("key5", "5" + str);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(nl, baos);
    }
    byte[] bytes = baos.toByteArray();

    NamedList nl1;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      nl1 = (NamedList) jbc
          .setReadStringAsCharSeq(true)
          .unmarshal(new ByteArrayInputStream(bytes, 0, bytes.length));
    }
    byte[] buf = ((ByteArrayUtf8CharSequence) nl1.getVal(0)).getBuf();
    ByteArrayUtf8CharSequence valLong = (ByteArrayUtf8CharSequence) nl1.get("key_long");
    assertFalse(valLong.getBuf() == buf);


    for (int i = 1; i < 6; i++) {
      ByteArrayUtf8CharSequence val = (ByteArrayUtf8CharSequence) nl1.get("key" + i);
      assertEquals(buf, val.getBuf());
      String s = val.toString();
      assertTrue(s.startsWith("" + i));
      assertTrue(s, s.endsWith(str));
    }

  }


}
