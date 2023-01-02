package com.flipkart.neo.solr.ltr.scorer.util;

import org.apache.lucene.search.ScoreDoc;

public class HeapUtil {

  /**
   * minheapAdjusts element at position pos.
   * @param docs minHeap with root at position zero.
   * @param size size of the minHeap
   * @param pos position which is to be adjusted.
   */
  public static void minHeapAdjust(ScoreDoc[] docs, int size, int pos) {
    final ScoreDoc doc = docs[pos];
    final float score = doc.score;
    int i = pos;
    while (i <= ((size >> 1) - 1)) {
      final int lchild = (i << 1) + 1;
      final ScoreDoc ldoc = docs[lchild];
      final float lscore = ldoc.score;
      float rscore = Float.MAX_VALUE;
      final int rchild = (i << 1) + 2;
      ScoreDoc rdoc = null;
      if (rchild < size) {
        rdoc = docs[rchild];
        rscore = rdoc.score;
      }
      if (lscore < score) {
        if (rscore < lscore) {
          docs[i] = rdoc;
          docs[rchild] = doc;
          i = rchild;
        } else {
          docs[i] = ldoc;
          docs[lchild] = doc;
          i = lchild;
        }
      } else if (rscore < score) {
        docs[i] = rdoc;
        docs[rchild] = doc;
        i = rchild;
      } else {
        return;
      }
    }
  }

  /**
   * creates minHeap with root at position 0.
   * @param docs docs.
   * @param size size of the heap.
   */
  public static void minHeapify(ScoreDoc[] docs, int size) {
    for (int i = (size >> 1) - 1; i >= 0; i--) {
      minHeapAdjust(docs, size, i);
    }
  }

}
