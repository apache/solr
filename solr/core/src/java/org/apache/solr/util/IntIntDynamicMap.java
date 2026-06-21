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

import java.util.Arrays;
import java.util.function.IntConsumer;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMapIndex;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

public class IntIntDynamicMap implements DynamicMap {
  private final int maxDoc;
  private int maxSize;
  private Int2IntOpenHashMapIndex hashMap;
  private GrowableWriter keyValues;
  private final int emptyValue;
  private final int threshold;

  /**
   * Create map with expected max value of key.
   * Although the map will automatically do resizing to be able to hold key {@code >= expectedKeyMax}.
   * But putting key much larger than {@code expectedKeyMax} is discourage since it can leads to use LOT OF memory.
   */
  public IntIntDynamicMap(int maxDoc, int expectedKeyMax, int emptyValue) {
    this.threshold = threshold(expectedKeyMax);
    this.maxSize = expectedKeyMax;
    this.maxDoc = maxDoc;
    this.emptyValue = emptyValue;
    int est = Math.max(0, expectedKeyMax == -1 ? maxDoc >> 2 : Math.min(maxDoc, expectedKeyMax));
    if (useArrayBased(maxDoc, est)) {
      upgradeToArray();
    } else {
      this.hashMap = new Int2IntOpenHashMapIndex(est);
     // this.hashMap.defaultReturnValue(emptyValue);
    }
  }

  private void upgradeToArray() {
   // keyValues = new int[maxDoc];
    keyValues = new GrowableWriter(PackedInts.bitsRequired(10000), 150000, PackedInts.FAST);
    if (emptyValue != 0) {
      for (var i = 0; i < keyValues.size(); i++) {
        keyValues.set(i, emptyValue);
      }
    }
//    if (emptyValue != 0) {
//      Arrays.fill(keyValues, emptyValue);
//    }
//    if (hashMap != null) {
//      hashMap.int2IntEntrySet().fastForEach(entry -> keyValues[entry.getIntKey() ] = entry.getIntValue());
//      hashMap = null;
//    }
  }
//
//  private void growBuffer(int minSize) {
//    assert keyValues != null;
//    int size = keyValues.length;
//    keyValues = ArrayUtil.grow(keyValues, minSize);
//    if (emptyValue != 0) {
//      for (int i = size; i < keyValues.length; i++) {
//        keyValues[i] = emptyValue;
//      }
//    }
//  }

  public void put(int key, int value) {
    if (keyValues != null) {
//      if (key >= keyValues.length) {
//        growBuffer(key + 1);
//      }
      keyValues.set(key, value);
    } else {
      this.maxSize = Math.max(key + 1, maxSize);
      this.hashMap.put(key, value);
//      if (this.hashMap.size() >= threshold) {
//        upgradeToArray();
//      }
    }
  }

  public int get(int key) {
    if (keyValues != null) {
      if (key >= keyValues.size()) {
        return emptyValue;
      }
      return (int) keyValues.get(key);
    } else {
      return this.hashMap.getOrDefault(key, emptyValue);
    }
  }

  public void forEachValue(IntConsumer consumer) {
    if (keyValues != null) {
      for (int i = 0; i < keyValues.size(); i++) {
        int val = (int) keyValues.get(i);
        if (val != emptyValue) consumer.accept(val);
      }
    } else {
      hashMap.int2IntEntrySet().fastForEach(ord -> consumer.accept(ord.getIntValue()));
    }
  }

  public void remove(int key) {
    if (keyValues != null) {
      if (key < keyValues.size())
        keyValues.set(key, emptyValue);
    } else {
      hashMap.remove(key);
    }
  }

}
