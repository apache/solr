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

import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.cursors.FloatCursor;
import com.carrotsearch.hppc.procedures.IntFloatProcedure;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import org.apache.lucene.util.ArrayUtil;

public class IntFloatDynamicMap implements DynamicMap {
  private final int maxDoc;
  private int maxSize;
  private Int2FloatOpenHashMap hashMap;
  private float[] keyValues;
  private float emptyValue;
  private int threshold;

  /**
   * Create map with expected max value of key.
   * Although the map will automatically do resizing to be able to hold key {@code >= expectedKeyMax}.
   * But putting key much larger than {@code expectedKeyMax} is discourage since it can leads to use LOT OF memory.
   */
  public IntFloatDynamicMap(int maxDoc, int expectedKeyMax, float emptyValue) {
    this.threshold = threshold(expectedKeyMax);
    this.maxSize = expectedKeyMax;
    this.maxDoc = maxDoc;
    this.emptyValue = emptyValue;
    int est = expectedKeyMax == -1 ? maxDoc >> 2 : Math.min(maxDoc, expectedKeyMax);
    if (useArrayBased(maxDoc, est)) {
      upgradeToArray();
    } else {
      this.hashMap = new Int2FloatOpenHashMap(mapExpectedElements(est));
    }
  }

  private void upgradeToArray() {
    keyValues = new float[maxDoc];
    if (emptyValue != 0.0f) {
      Arrays.fill(keyValues, emptyValue);
    }
    if (hashMap != null) {
      hashMap.int2FloatEntrySet().fastForEach(entry -> keyValues[entry.getIntKey()] = entry.getFloatValue());
      hashMap = null;
    }
  }

  private void growBuffer(int minSize) {
    assert keyValues != null;
    int size = keyValues.length;
    keyValues = ArrayUtil.grow(keyValues, minSize);
    if (emptyValue != 0.0f) {
      for (int i = size; i < keyValues.length; i++) {
        keyValues[i] = emptyValue;
      }
    }
  }

  public void put(int key, float value) {
    if (keyValues != null) {
      if (key >= keyValues.length) {
        growBuffer(key + 1);
      }
      keyValues[key] = value;
    } else {
      this.hashMap.put(key, value);
      this.maxSize = Math.max(key+1, maxSize);
      if (this.hashMap.size() >= threshold) {
        upgradeToArray();
      }
    }
  }

  public float get(int key) {
    if (keyValues != null) {
      if (key >= keyValues.length) {
        return emptyValue;
      }
      return keyValues[key];
    } else {
      return this.hashMap.getOrDefault(key, emptyValue);
    }
  }

  public void forEachValue(FloatConsumer consumer) {
    if (keyValues != null) {
      for (float val : keyValues) {
        if (val != emptyValue) consumer.accept(val);
      }
    } else {
      hashMap.int2FloatEntrySet().fastForEach(entry -> consumer.accept(entry.getFloatValue()));
    }
  }

  public void remove(int key) {
    if (keyValues != null) {
      if (key < keyValues.length)
        keyValues[key] = emptyValue;
    } else {
      hashMap.remove(key);
    }
  }
}
