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
package org.apache.solr.search;

import com.carrotsearch.hppc.IntHashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/** A simple sorted int[] array implementation of {@link DocSet}, good for small sets. */
public class SortedIntDocSet extends DocSet {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SortedIntDocSet.class)
          + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  protected final int[][] docs;
  final int capacity;

  /**
   * @param docs Sorted list of ids
   */
  public SortedIntDocSet(int[][] docs) {
    this.docs = docs;
    this.capacity = getCapacity(docs);
  }

  static int getCapacity(int[][] docs) {
    int lastOuterIdx = docs.length - 1;
    if (lastOuterIdx == -1) {
      return 0;
    } else {
      return docs[lastOuterIdx].length + (lastOuterIdx * MAX_ARR_SIZE);
    }
  }

  /**
   * @param docs Sorted list of ids
   * @param len Number of ids in the list
   */
  public SortedIntDocSet(int[][] docs, int len) {
    this(shrink(docs, len));
  }

  public static int[][] grow(int[][] buffer, int limit, int newSize) {
    int[][] ret = allocate(newSize);
    if (limit <= 0) return ret;
    int lastIdx = limit - 1;
    int i = lastIdx >> SortedIntDocSet.WORDS_SHIFT;
    System.arraycopy(buffer[i], 0, ret[i], 0, (lastIdx & SortedIntDocSet.ARR_MASK) + 1);
    while (--i >= 0) {
      ret[i] = buffer[i];
    }
    return ret;
  }

  public int[][] getDocs() {
    return docs;
  }

  @Override
  public int size() {
    return capacity;
  }

  public static int[][] zeroInts = new int[0][];
  public static SortedIntDocSet zero = new SortedIntDocSet(zeroInts);

  static final int WORDS_SHIFT =
      FixedBitSet.WORDS_SHIFT + 1; // +1 b/c bytes(int[] * 2) == bytes(long[])
  static final int MAX_ARR_SIZE = 1 << WORDS_SHIFT;
  static final int ARR_MASK = MAX_ARR_SIZE - 1;

  public static int[][] allocate(int size) {
    if (size <= 0) return zeroInts;
    int outerSize = ((size - 1) >> WORDS_SHIFT) + 1;
    int[][] ret = new int[outerSize][];
    int i = outerSize - 1;
    ret[i] = new int[((size - 1) & ARR_MASK) + 1];
    while (--i >= 0) {
      ret[i] = new int[MAX_ARR_SIZE];
    }
    return ret;
  }

  public static int[][] shrink(int[][] arr, int newSize) {
    if (newSize == 0) return zeroInts;
    if (getCapacity(arr) == newSize) return arr;
    int outerSize = ((newSize - 1) >> WORDS_SHIFT) + 1;
    int[][] newArr = new int[outerSize][];
    int i = outerSize - 1;
    int lastIdxSize = ((newSize - 1) & ARR_MASK) + 1;
    int[] lastIdxArr = new int[lastIdxSize];
    newArr[i] = lastIdxArr;
    System.arraycopy(arr[i], 0, lastIdxArr, 0, lastIdxSize);
    while (--i >= 0) {
      // share content; careful!
      newArr[i] = arr[i];
    }
    return newArr;
  }

  public static int[][] shrinkClone(int[][] arr, int newSize) {
    if (newSize == 0) return zeroInts;
    int outerSize = ((newSize - 1) >> WORDS_SHIFT) + 1;
    int[][] newArr = new int[outerSize][];
    int i = outerSize - 1;
    int lastIdxSize = ((newSize - 1) & ARR_MASK) + 1;
    int[] lastIdxArr = new int[lastIdxSize];
    newArr[i] = lastIdxArr;
    System.arraycopy(arr[i], 0, lastIdxArr, 0, lastIdxSize);
    while (--i >= 0) {
      // don't share content
      newArr[i] = arr[i].clone();
    }
    return newArr;
  }

  public static int intersectionSize(
      SortedIntDocSet smallerSortedList, SortedIntDocSet biggerSortedList) {
    final int[][] a = smallerSortedList.docs;
    final int[][] b = biggerSortedList.docs;

    // The next doc we are looking for will be much closer to the last position we tried
    // than it will be to the midpoint between last and high... so probe ahead using
    // a function of the ratio of the sizes of the sets.
    int step = (biggerSortedList.capacity / smallerSortedList.capacity) + 1;

    // Since the majority of probes should be misses, we'll already be above the last probe
    // and shouldn't need to move larger than the step size on average to step over our target (and
    // thus lower the high upper bound a lot.)... but if we don't go over our target, it's a big
    // miss... so double it.
    step = step + step;

    // FUTURE: come up with a density such that target * density == likely position?
    // then check step on one side or the other?
    // (density could be cached in the DocSet)... length/maxDoc

    // FUTURE: try partitioning like a sort algorithm.  Pick the midpoint of the big
    // array, find where that should be in the small array, and then recurse with
    // the top and bottom half of both arrays until they are small enough to use
    // a fallback intersection method.
    // NOTE: I tried this and it worked, but it was actually slower than this current
    // highly optimized approach.

    int icount = 0;
    int low = 0;
    int max = biggerSortedList.capacity - 1;

    for (int i = 0; i < smallerSortedList.capacity; i++) {
      int doca = a[i >> WORDS_SHIFT][i & ARR_MASK];

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search the rest of the way
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT][mid & ARR_MASK];

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          icount++;
          low = mid + 1; // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  public static boolean intersects(
      SortedIntDocSet smallerSortedList, SortedIntDocSet biggerSortedList) {
    // see intersectionSize for more in-depth comments of this algorithm

    final int[][] a = smallerSortedList.docs;
    final int[][] b = biggerSortedList.docs;

    int step = (biggerSortedList.capacity / smallerSortedList.capacity) + 1;

    step = step + step;

    int low = 0;
    int max = biggerSortedList.capacity - 1;

    for (int i = 0; i < smallerSortedList.capacity; i++) {
      int doca = a[i >> WORDS_SHIFT][i & ARR_MASK];
      int high = max;
      int probe = low + step;
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
          high = probe;
        } else {
          low = probe + 1;
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT][mid & ARR_MASK];

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      // BitDocSet is  better at random access than we are
      int icount = 0;
      for (int i = 0; i < docs.length; i++) {
        int[] sub = docs[i];
        for (int j = 0, lim = sub.length; j < lim; j++) {
          if (other.exists(sub[j])) icount++;
        }
      }
      return icount;
    }

    // make "a" the smaller set.
    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    final SortedIntDocSet a = capacity < otherSet.capacity ? this : otherSet;
    final SortedIntDocSet b = capacity < otherSet.capacity ? otherSet : this;

    if (a.capacity == 0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.capacity >> 3) >= a.capacity) {
      return intersectionSize(a, b);
    }

    // if they are close in size, just do a linear walk of both.
    int icount = 0;
    int i = 0, j = 0;
    int[][] aDocs = a.docs;
    int[][] bDocs = b.docs;
    int doca = aDocs[i >> WORDS_SHIFT][i & ARR_MASK], docb = bDocs[j >> WORDS_SHIFT][j & ARR_MASK];
    for (; ; ) {
      // switch on the sign bit somehow? Hopefully JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT][j & ARR_MASK];
      } else if (doca < docb) {
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT][i & ARR_MASK];
      } else {
        icount++;
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT][i & ARR_MASK];
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT][j & ARR_MASK];
      }
    }
    return icount;
  }

  @Override
  public boolean intersects(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      // assume BitDocSet is better at random access than we are
      for (int[] sub : docs) {
        for (int doc : sub) {
          if (other.exists(doc)) return true;
        }
      }
      return false;
    }

    // make "a" the smaller set.
    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    final SortedIntDocSet a = capacity < otherSet.capacity ? this : otherSet;
    final SortedIntDocSet b = capacity < otherSet.capacity ? otherSet : this;

    if (a.capacity == 0) return false;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.capacity >> 3) >= a.capacity) {
      return intersects(a, b);
    }

    // if they are close in size, just do a linear walk of both.
    int i = 0, j = 0;
    int[][] aDocs = a.docs;
    int[][] bDocs = b.docs;
    int doca = aDocs[i >> WORDS_SHIFT][i & ARR_MASK], docb = bDocs[j >> WORDS_SHIFT][j & ARR_MASK];
    for (; ; ) {
      // switch on the sign bit somehow?  Hopefull JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT][j & ARR_MASK];
      } else if (doca < docb) {
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT][i & ARR_MASK];
      } else {
        return true;
      }
    }
    return false;
  }

  /** puts the intersection of a and b into the target array and returns the size */
  public static int intersection(int[][] a, int lena, int[][] b, int lenb, int[][] target) {
    if (lena > lenb) {
      int ti = lena;
      lena = lenb;
      lenb = ti;
      int[][] ta = a;
      a = b;
      b = ta;
    }

    if (lena == 0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb >> 3) >= lena) {
      return intersectionBinarySearch(a, lena, b, lenb, target);
    }

    int icount = 0;
    int i = 0, j = 0;
    int doca = a[i >> WORDS_SHIFT][i & ARR_MASK], docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
    for (; ; ) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
      } else if (doca < docb) {
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT][i & ARR_MASK];
      } else {
        target[icount >> WORDS_SHIFT][icount++ & ARR_MASK] = doca;
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT][i & ARR_MASK];
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
      }
    }
    return icount;
  }

  /**
   * Puts the intersection of a and b into the target array and returns the size. lena should be
   * smaller than lenb
   */
  protected static int intersectionBinarySearch(
      int[][] a, int lena, int[][] b, int lenb, int[][] target) {
    int step = (lenb / lena) + 1;
    step = step + step;

    int icount = 0;
    int low = 0;
    int max = lenb - 1;

    for (int i = 0; i < lena; i++) {
      int doca = a[i >> WORDS_SHIFT][i & ARR_MASK];

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT][mid & ARR_MASK];

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          target[icount >> WORDS_SHIFT][icount++ & ARR_MASK] = doca;
          low = mid + 1; // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  @Override
  public DocSet intersection(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      int icount = 0;
      int[][] arr = allocate(capacity);
      for (int i = 0; i < capacity; i++) {
        int doc = docs[i >> WORDS_SHIFT][i & ARR_MASK];
        if (other.exists(doc)) {
          arr[icount >> WORDS_SHIFT][icount & ARR_MASK] = doc;
          icount++;
        }
      }
      if (icount == capacity) {
        return this; // no change
      }
      return new SortedIntDocSet(arr, icount);
    }

    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    int maxsz = Math.min(capacity, otherSet.capacity);
    int[][] arr = allocate(maxsz);
    int sz = intersection(docs, capacity, otherSet.docs, otherSet.capacity, arr);
    if (sz == capacity) {
      return this; // no change
    }
    return new SortedIntDocSet(arr, sz);
  }

  protected static int andNotBinarySearch(
      int[][] a, int lena, int[][] b, int lenb, int[][] target) {
    int step = (lenb / lena) + 1;
    step = step + step;

    int count = 0;
    int low = 0;
    int max = lenb - 1;

    outer:
    for (int i = 0; i < lena; i++) {
      int doca = a[i >> WORDS_SHIFT][i & ARR_MASK];

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT][probe & ARR_MASK] >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT][mid & ARR_MASK];

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          low = mid + 1; // found it, so start at next element
          continue outer;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
      target[count >> WORDS_SHIFT][count++ & ARR_MASK] = doca;
    }

    return count;
  }

  /** puts the intersection of a and not b into the target array and returns the size */
  public static int andNot(int[][] a, int lena, int[][] b, int lenb, int[][] target) {
    if (lena == 0) return 0;
    if (lenb == 0) {
      int i = lena >> WORDS_SHIFT;
      System.arraycopy(a[i], 0, target[i], 0, lena & ARR_MASK);
      while (--i >= 0) {
        System.arraycopy(a[i], 0, target[i], 0, MAX_ARR_SIZE);
      }
      return lena;
    }

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb >> 3) >= lena) {
      return andNotBinarySearch(a, lena, b, lenb, target);
    }

    int count = 0;
    int i = 0, j = 0;
    int doca = a[i >> WORDS_SHIFT][i & ARR_MASK], docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
    for (; ; ) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
      } else if (doca < docb) {
        target[count >> WORDS_SHIFT][count++ & ARR_MASK] = doca;
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT][i & ARR_MASK];
      } else {
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT][i & ARR_MASK];
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT][j & ARR_MASK];
      }
    }

    int leftover = lena - i;

    if (leftover > 0) {
      arraycopy(a, i, target, count, leftover);
      count += leftover;
    }

    return count;
  }

  public static void arraycopy(int[][] src, int srcIdx, int[][] dest, int destIdx, int len) {
    if (len == 0) return;
    int srcOuterOffset = srcIdx >> WORDS_SHIFT;
    final int destOuterOffset = destIdx >> WORDS_SHIFT;
    int srcInnerOffset = srcIdx & ARR_MASK;
    int destInnerOffset = destIdx & ARR_MASK;
    final int len1;
    final int len2;
    int[] srcArr1;
    int[] srcArr2;

    // the array offset of the word for the last "bit" element.
    final int destOuterLimit = (destIdx + len - 1) >> WORDS_SHIFT;

    if (srcInnerOffset <= destInnerOffset) {
      len1 = destInnerOffset - srcInnerOffset;
      len2 = MAX_ARR_SIZE - len1;
      srcArr1 = null;
      srcArr2 = src[srcOuterOffset];
    } else {
      len2 = srcInnerOffset - destInnerOffset;
      len1 = MAX_ARR_SIZE - len2;
      srcArr1 = src[srcOuterOffset]; // clear out-of-scope bits
      srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;
    }
    // special handling for the first word, which may be partial
    int[] destArr = dest[destOuterOffset];
    if (srcArr1 == null) {
      System.arraycopy(
          srcArr2,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, MAX_ARR_SIZE - destInnerOffset));
    } else if (srcArr2 == null) {
      System.arraycopy(
          srcArr1,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, MAX_ARR_SIZE - srcInnerOffset));
    } else {
      int initialLen = MAX_ARR_SIZE - srcInnerOffset;
      if (len <= initialLen) {
        System.arraycopy(srcArr1, srcInnerOffset, destArr, destInnerOffset, len);
      } else {
        System.arraycopy(srcArr1, srcInnerOffset, destArr, destInnerOffset, initialLen);
        System.arraycopy(
            srcArr2, 0, destArr, destInnerOffset + initialLen, Math.min(len2, len - initialLen));
      }
    }
    if (destOuterOffset == destOuterLimit) return;

    for (int i = destOuterOffset + 1; i < destOuterLimit; i++) {
      // inner words are guaranteed to not be partial, so this can be very simple
      srcArr1 = srcArr2;
      srcArr2 = src[++srcOuterOffset];
      destArr = dest[i];
      System.arraycopy(srcArr1, len2, destArr, 0, len1);
      System.arraycopy(srcArr2, 0, destArr, len1, len2);
    }
    srcArr1 = srcArr2;
    srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;

    // special handling for the last word, which may be partial
    int remainder = ((destIdx + len - 1) & ARR_MASK) + 1;
    destArr = dest[destOuterLimit];
    if (srcArr2 == null || remainder <= len1) {
      System.arraycopy(srcArr1, len2, destArr, 0, remainder);
    } else {
      System.arraycopy(srcArr1, len2, destArr, 0, len1);
      System.arraycopy(srcArr2, 0, destArr, len1, remainder - len1);
    }
  }

  @Override
  public DocSet andNot(DocSet other) {
    if (other.size() == 0) return this;

    if (!(other instanceof SortedIntDocSet)) {
      int count = 0;
      int[][] arr = allocate(capacity);
      for (int i = 0; i < capacity; i++) {
        int doc = docs[i >> WORDS_SHIFT][i & ARR_MASK];
        if (!other.exists(doc)) arr[count >> WORDS_SHIFT][count++ & ARR_MASK] = doc;
      }
      if (count == capacity) {
        return this; // no change
      }
      return new SortedIntDocSet(arr, count);
    }

    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    int[][] arr = allocate(capacity);
    int sz = andNot(docs, capacity, otherSet.docs, otherSet.capacity, arr);
    if (sz == capacity) {
      return this; // no change
    }
    return new SortedIntDocSet(arr, sz);
  }

  @Override
  public void addAllTo(FixedBitSet target) {
    for (int[] sub : docs) {
      for (int doc : sub) {
        target.set(doc);
      }
    }
  }

  @Override
  public boolean exists(int doc) {
    // this could be faster by estimating where in the list the doc is likely to appear,
    // but we should get away from using exists() anyway.
    int low = 0;
    int high = capacity - 1;
    // binary search
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int docb = docs[mid >> WORDS_SHIFT][mid & ARR_MASK];

      if (docb < doc) {
        low = mid + 1;
      } else if (docb > doc) {
        high = mid - 1;
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      int pos = 0;

      @Override
      public boolean hasNext() {
        return pos < capacity;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      /** The remove operation is not supported by this Iterator. */
      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "The remove  operation is not supported by this Iterator.");
      }

      @Override
      public int nextDoc() {
        return docs[pos >> WORDS_SHIFT][pos++ & ARR_MASK];
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  @Override
  public Bits getBits() {
    IntHashSet hashSet = new IntHashSet(capacity);
    for (int[] sub : docs) {
      for (int doc : sub) {
        hashSet.add(doc);
      }
    }

    return new Bits() {
      @Override
      public boolean get(int index) {
        return hashSet.contains(index);
      }

      @Override
      public int length() {
        return getLength();
      }
    };
  }

  /** the {@link Bits#length()} or maxdoc (1 greater than largest possible doc number) */
  private int getLength() {
    int size = size();
    if (size == 0) {
      return 0;
    } else {
      int idx = size - 1;
      return getDocs()[idx >> WORDS_SHIFT][idx & ARR_MASK] + 1;
    }
  }

  @Override
  protected FixedBitSet getFixedBitSet() {
    return getFixedBitSetClone();
  }

  @Override
  protected FixedBitSet getFixedBitSetClone() {
    FixedBitSet bitSet = new FixedBitSet(getLength());
    addAllTo(bitSet);
    return bitSet;
  }

  @Override
  public DocSet union(DocSet other) {
    // TODO could be more efficient if both are SortedIntDocSet
    FixedBitSet otherBits = other.getFixedBitSet();
    FixedBitSet newbits = FixedBitSet.ensureCapacity(getFixedBitSetClone(), otherBits.length());
    newbits.or(otherBits);
    return new BitDocSet(newbits);
  }

  private volatile int[] cachedOrdIdxMap; // idx of first doc _beyond_ the corresponding seg

  private int[] getOrdIdxMap(LeafReaderContext ctx) {
    final int[] cached = cachedOrdIdxMap;
    if (cached != null) {
      return cached;
    } else {
      List<LeafReaderContext> leaves = ReaderUtil.getTopLevelContext(ctx).leaves();
      final int[] ret = new int[leaves.size()];
      int lastLimit = 0;
      int lastLimitDoc = docs[0][0]; // capacity != 0
      for (LeafReaderContext lrc : leaves) {
        // sanity check that initial `lastLimit*` values are valid (and consequently that our
        // initial setting of `startIdx` for context.ord==0 won't inadvertently include invalid
        // docs).
        assert lrc.ord != 0 || lastLimitDoc >= lrc.docBase;
        final int max =
            lrc.docBase + lrc.reader().maxDoc(); // one past the max doc in this segment.
        if (lastLimitDoc >= max) {
          ret[lrc.ord] = lastLimit;
          continue;
        }
        assert lastLimitDoc >= lrc.docBase;
        final int nextLimit = binarySearch(docs, lastLimit + 1, capacity, max);
        lastLimit = nextLimit < 0 ? ~nextLimit : nextLimit;
        lastLimitDoc =
            lastLimit < capacity
                ? docs[lastLimit >> WORDS_SHIFT][lastLimit & ARR_MASK]
                : DocIdSetIterator.NO_MORE_DOCS;
        ret[lrc.ord] = lastLimit;
      }
      return cachedOrdIdxMap = ret; // set/replace atomically after building
    }
  }

  @Override
  public DocIdSetIterator iterator(LeafReaderContext context) {

    if (capacity == 0 || context.reader().maxDoc() < 1) {
      // empty docset or entirely empty segment (verified that the latter actually happens)
      // NOTE: wrt the "empty docset" case, this is not just an optimization; this shortcircuits
      // also to prevent the static DocSet.EmptyLazyHolder.INSTANCE from having cachedOrdIdxMap
      // initiated across different IndexReaders.
      return null;
    }

    final int startIdx;
    final int limitIdx;
    if (context.isTopLevel) {
      startIdx = 0;
      limitIdx = capacity;
    } else {
      int[] ordIdxMap = getOrdIdxMap(context);
      startIdx = context.ord == 0 ? 0 : ordIdxMap[context.ord - 1];
      limitIdx = ordIdxMap[context.ord];

      if (startIdx >= limitIdx) {
        return null; // verified this does happen
      }
    }
    final int base = context.docBase;

    return new DocIdSetIterator() {
      int idx = startIdx - 1;
      int adjustedDoc = -1;

      @Override
      public int docID() {
        return adjustedDoc;
      }

      @Override
      public int nextDoc() {
        return adjustedDoc =
            (++idx >= limitIdx) ? NO_MORE_DOCS : (docs[idx >> WORDS_SHIFT][idx & ARR_MASK] - base);
      }

      @Override
      public int advance(int target) {
        if (++idx >= limitIdx || target == NO_MORE_DOCS) return adjustedDoc = NO_MORE_DOCS;
        target += base;

        // probe next
        int rawDoc = docs[idx >> WORDS_SHIFT][idx & ARR_MASK];
        if (rawDoc >= target) return adjustedDoc = rawDoc - base;

        // TODO: probe more before resorting to binary search?

        final int findIdx = binarySearch(docs, idx + 1, limitIdx, target);
        idx = findIdx < 0 ? ~findIdx : findIdx;
        return adjustedDoc =
            idx < limitIdx ? docs[idx >> WORDS_SHIFT][idx & ARR_MASK] - base : NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return (long) limitIdx - startIdx;
      }
    };
  }

  public static int binarySearch(int[][] a, int fromIndex, int toIndex, int key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid >> WORDS_SHIFT][mid & ARR_MASK];

      if (midVal < key) low = mid + 1;
      else if (midVal > key) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  @Override
  public DocSetQuery makeQuery() {
    return new DocSetQuery(this);
  }

  @Override
  public SortedIntDocSet clone() {
    int[][] newDocs = new int[docs.length][];
    for (int i = docs.length - 1; i >= 0; i--) {
      newDocs[i] = docs[i].clone();
    }
    return new SortedIntDocSet(newDocs);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + ((long) capacity << 2);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return "SortedIntDocSet{"
        + "size="
        + size()
        + ","
        + "ramUsed="
        + RamUsageEstimator.humanReadableUnits(ramBytesUsed())
        + '}';
  }
}
