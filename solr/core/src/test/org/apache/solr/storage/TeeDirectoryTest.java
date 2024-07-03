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
package org.apache.solr.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class TeeDirectoryTest extends SolrTestCase {

  @Test
  public void testIsLazyTmpFile() {
    String[] lazyTmp =
        new String[] {
          "_lazy_123.tmp", "_lazy_1.tmp", "_lazy_123abcdefghijklmnopqrstuvwxyz0456789.tmp"
        };
    String[] notLazyTmp =
        new String[] {"", "a", "lazy_123.tmp", "_lazy_.tmp", "_lazy_Q.tmp", "_lazy_123.tmpp"};
    String prefix = "prefix";
    int prefixLen = prefix.length();
    for (String s : lazyTmp) {
      assertEquals(0, AccessDirectory.lazyTmpFileSuffixStartIdx(s));
      assertEquals(prefixLen, AccessDirectory.lazyTmpFileSuffixStartIdx(prefix.concat(s)));
    }
    for (String s : notLazyTmp) {
      assertEquals(-1, AccessDirectory.lazyTmpFileSuffixStartIdx(s));
      assertEquals(-1, AccessDirectory.lazyTmpFileSuffixStartIdx("prefix".concat(s)));
    }
  }

  @Test
  public void testSortAndMergeArrays() {
    Random r = random();
    Set<String> a = new HashSet<>();
    Set<String> b = new HashSet<>();
    SortedSet<String> sorted = new TreeSet<>();
    for (int i = 0; i < 1000; i++) {
      a.clear();
      b.clear();
      sorted.clear();
      for (int j = r.nextInt(14); j > 0; j--) {
        String s = Character.toString('a' + r.nextInt(26));
        if (r.nextInt(10) == 0) {
          s = s.concat("_lazy_1.tmp");
        } else {
          sorted.add(s);
        }
        a.add(s);
      }
      for (int j = r.nextInt(14); j > 0; j--) {
        String s = Character.toString('a' + r.nextInt(26));
        sorted.add(s);
        b.add(s);
      }
      String[] aArr = a.toArray(new String[0]);
      String[] bArr = b.toArray(new String[0]);
      Collections.shuffle(Arrays.asList(aArr), r);
      Collections.shuffle(Arrays.asList(bArr), r);
      String[] result = TeeDirectory.sortAndMergeArrays(aArr, bArr);
      Arrays.sort(result);
      assertArrayEquals(sorted.toArray(new String[0]), result);
    }
  }
}
