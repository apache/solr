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
package org.apache.solr.search.facet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.search.facet.FacetRequest.RefineMethod.SIMPLE;


public abstract class FacetMerger {
  public abstract void merge(Object facetResult, Context mcontext);

  // FIXME
  //  public abstract Map<String,Object> getRefinement(Context mcontext);
  public Map<String,Object> getRefinement(Context mcontext) {
    return null;
  }
  public abstract void finish(Context mcontext);
  public abstract Object getMergedResult();  // TODO: we should pass mcontext through here as well

  // This class lets mergers know overall context such as what shard is being merged
  // and what buckets have been seen by what shard.
  public static class Context {
    // FacetComponentState state;  // todo: is this needed?
    final int numShards;
    private final BitSet sawShard = new BitSet(); // [bucket0_shard0, bucket0_shard1, bucket0_shard2,  bucket1_shard0, bucket1_shard1, bucket1_shard2]
    private Map<String,Integer> shardmap = new HashMap<>();
    private int pass = 0;

    public Context(int numShards) {
      this.numShards = numShards;
    }

    Object root;  // per-shard response
    int maxBucket;  // the current max bucket across all bucket types... incremented as we encounter more
    int shardNum = -1;  // TODO: keep same mapping across multiple phases...
    boolean bucketWasMissing;

    public void newShard(String shard) {
      Integer prev = shardmap.put(shard, ++shardNum);
      assert prev == null;
      this.bucketWasMissing = false;
    }

    public int incrementPass() {
      return ++pass;
    }

    public int getPass() {
      return pass;
    }

    public void setShard(String shard) {
      this.shardNum = shardmap.get(shard);
    }

    public int getNewBucketNumber() {
      return maxBucket++;
    }

    public void setShardFlag(int bucketNum) {
      // rely on normal bitset expansion (uses a doubling strategy)
      sawShard.set( bucketNum * numShards + shardNum );
    }

    public boolean getShardFlag(int bucketNum, int shardNum) {
      return sawShard.get( bucketNum * numShards + shardNum );
    }

    public boolean getShardFlag(int bucketNum) {
      return getShardFlag(bucketNum, shardNum);
    }

    public boolean bucketWasMissing() {
      return bucketWasMissing;
    }

    public boolean setBucketWasMissing(boolean newVal) {
      boolean oldVal = bucketWasMissing();
      bucketWasMissing = newVal;
      return oldVal;
    }

    private Map<FacetRequest, Collection<String>> refineSubMap = new IdentityHashMap<>(4);
    public Collection<String> getSubsWithRefinement(FacetRequest freq) {
      if (freq.getSubFacets().isEmpty()) return Collections.emptyList();
      Collection<String> subs = refineSubMap.get(freq);
      if (subs != null) return subs;

      for (Map.Entry<String,FacetRequest> entry : freq.subFacets.entrySet()) {
        Collection<String> childSubs = getSubsWithRefinement(entry.getValue());
        if (childSubs.size() > 0 || entry.getValue().getRefineMethod() == SIMPLE) {
          if (subs == null) {
            subs = new ArrayList<>(freq.getSubFacets().size());
          }
          subs.add(entry.getKey());
        }
      }

      if (subs == null) {
        subs = Collections.emptyList();
      }
      refineSubMap.put(freq, subs);
      return subs;
    }

    protected enum TopLevelSub { NONE, DESCENDANT, CHILD }
    protected static final Map<TopLevelSub, Collection<String>> NONE_ENTRY;
    static {
      Map<TopLevelSub, Collection<String>> tmp = new EnumMap<>(TopLevelSub.class);
      tmp.put(TopLevelSub.NONE, null);
      NONE_ENTRY = Collections.unmodifiableMap(tmp);
    }

    private Map<FacetRequest, Map<TopLevelSub, Collection<String>>> hasTopLevelSubs = new IdentityHashMap<>(4);
    public Map<TopLevelSub, Collection<String>> hasTopLevelSubs(FacetRequest freq) {
      if (freq.getSubFacets().isEmpty()) return NONE_ENTRY;
      Map<TopLevelSub, Collection<String>> cached = hasTopLevelSubs.get(freq);
      if (cached != null) {
        return cached;
      }

      final Set<Map.Entry<String, FacetRequest>> children = freq.subFacets.entrySet();
      Collection<String> topLevelChildren = new ArrayList<>(children.size());
      Collection<String> childrenWithTopLevelDescendants = new ArrayList<>(children.size());
      for (Map.Entry<String,FacetRequest> entry : children) {
        final FacetRequest child = entry.getValue();
        if (child.evaluateAsTopLevel()) {
          topLevelChildren.add(entry.getKey());
        } else if (hasTopLevelSubs(child) != NONE_ENTRY) {
          childrenWithTopLevelDescendants.add(entry.getKey());
        }
      }
      if (topLevelChildren.isEmpty() && childrenWithTopLevelDescendants.isEmpty()) {
        // the common case
        hasTopLevelSubs.put(freq, NONE_ENTRY);
        return NONE_ENTRY;
      }
      Map<TopLevelSub, Collection<String>> ret = new EnumMap<>(TopLevelSub.class);
      if (!topLevelChildren.isEmpty()) {
        ret.put(TopLevelSub.CHILD, topLevelChildren);
      }
      if (!childrenWithTopLevelDescendants.isEmpty()) {
        ret.put(TopLevelSub.DESCENDANT, childrenWithTopLevelDescendants);
      }
      hasTopLevelSubs.put(freq, ret);
      return ret;
    }


    private Map<FacetRequest, Collection<String>> partialSubsMap = new IdentityHashMap<>(4);
    public Collection<String> getSubsWithPartial(FacetRequest freq) {
      if (freq.getSubFacets().isEmpty()) return Collections.emptyList();
      Collection<String> subs = partialSubsMap.get(freq);
      if (subs != null) return subs;

      subs = null;
      for (Map.Entry<String,FacetRequest> entry : freq.subFacets.entrySet()) {
        final FacetRequest entryVal = entry.getValue();
        Collection<String> childSubs = getSubsWithPartial(entryVal);
        // TODO: should returnsPartial() check processEmpty internally?
        if (childSubs.size() > 0 || entryVal.returnsPartial() || entryVal.processEmpty) {
          if (subs == null) {
            subs = new ArrayList<>(freq.getSubFacets().size());
          }
          subs.add(entry.getKey());
        }
      }

      if (subs == null) {
        subs = Collections.emptyList();
      }
      partialSubsMap.put(freq, subs);
      return subs;
    }


  }



}


