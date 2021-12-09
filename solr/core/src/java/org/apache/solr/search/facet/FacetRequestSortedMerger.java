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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.util.SimpleOrderedMap;

// base class for facets that create a list of buckets that can be sorted
abstract class FacetRequestSortedMerger<FacetRequestT extends FacetRequestSorted> extends FacetModule.FacetBucketMerger<FacetRequestT> {
  LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<>();
  List<FacetBucket> sortedBuckets;
  BitSet shardHasMoreBuckets;  // null, or "true" if we saw a result from this shard and it indicated that there are more results
  Context mcontext;  // HACK: this should be passed in getMergedResult as well!

  public FacetRequestSortedMerger(FacetRequestT freq) {
    super(freq);
  }

  @Override
  public void merge(Object facetResult, Context mcontext) {
    this.mcontext = mcontext;
    SimpleOrderedMap<?> res = (SimpleOrderedMap<?>)facetResult;
    Boolean more = (Boolean)res.get("more");
    if (more != null && more) {
      if (shardHasMoreBuckets == null) {
        // We really only need this if it's a partial facet (has a limit)
        shardHasMoreBuckets = new BitSet(mcontext.numShards);
      }
      shardHasMoreBuckets.set(mcontext.shardNum);
    }
  }

  private static class SortVal implements Comparable<SortVal> {
    FacetBucket bucket;
    FacetModule.FacetSortableMerger merger;  // make this class inner and access merger , direction in parent?
    FacetRequest.SortDirection direction;

    @Override
    @SuppressWarnings({"unchecked"})
    public int compareTo(SortVal o) {
      int c = -merger.compareTo(o.merger, direction) * direction.getMultiplier();
      return c == 0 ? bucket.bucketValue.compareTo(o.bucket.bucketValue) : c;
    }
  }

  public void mergeBucketList(List<SimpleOrderedMap<?>> bucketList, Context mcontext) {
    for (SimpleOrderedMap<?> bucketRes : bucketList) {
      @SuppressWarnings("rawtypes")
      Comparable bucketVal = (Comparable)bucketRes.get("val");
      FacetBucket bucket = buckets.get(bucketVal);
      if (bucket == null) {
        bucket = newBucket(bucketVal, mcontext);
        buckets.put(bucketVal, bucket);
      }
      bucket.mergeBucket( bucketRes , mcontext );
    }
  }


  @SuppressWarnings({"unchecked"})
  public void sortBuckets(final FacetRequest.FacetSort sort) {
    // NOTE: we *always* re-init from buckets, because it may have been modified post-refinement 
    sortedBuckets = new ArrayList<>( buckets.values() );

    Comparator<FacetBucket> comparator = null;

    final FacetRequest.SortDirection direction = sort.sortDirection;
    final int sortMul = direction.getMultiplier();

    if ("count".equals(sort.sortVariable)) {
      comparator = (o1, o2) -> {
        int v = -Long.compare(o1.count, o2.count) * sortMul;
        return v == 0 ? o1.bucketValue.compareTo(o2.bucketValue) : v;
      };
      Collections.sort(sortedBuckets, comparator);
    } else if ("index".equals(sort.sortVariable)) {
      comparator = (o1, o2) -> -o1.bucketValue.compareTo(o2.bucketValue) * sortMul;
      Collections.sort(sortedBuckets, comparator);
    } else {
      final String key = sort.sortVariable;

      /**
       final FacetSortableMerger[] arr = new FacetSortableMerger[buckets.size()];
       final int[] index = new int[arr.length];
       int start = 0;
       int nullStart = index.length;
       int i=0;
       for (FacetBucket bucket : buckets.values()) {
       FacetMerger merger = bucket.getExistingMerger(key);
       if (merger == null) {
       index[--nullStart] = i;
       }
       if (merger != null) {
       arr[start] = (FacetSortableMerger)merger;
       index[start] = i;
       start++;
       }
       i++;
       }

       PrimUtils.sort(0, nullStart, index, new PrimUtils.IntComparator() {
      @Override
      public int compare(int a, int b) {
      return arr[index[a]].compareTo(arr[index[b]], direction);
      }
      });
       **/


      List<SortVal> lst = new ArrayList<>(buckets.size());
      List<FacetBucket> nulls = new ArrayList<>(buckets.size()>>1);
      for (int i=0; i<sortedBuckets.size(); i++) {
        FacetBucket bucket = sortedBuckets.get(i);
        FacetMerger merger = bucket.getExistingMerger(key);
        if (merger == null) {
          nulls.add(bucket);
        }
        if (merger != null) {
          SortVal sv = new SortVal();
          sv.bucket = bucket;
          sv.merger = (FacetModule.FacetSortableMerger)merger;
          sv.direction = direction;
          // sv.pos = i;  // if we need position in the future...
          lst.add(sv);
        }
      }
      Collections.sort(lst);
      Collections.sort(nulls, (o1, o2) -> o1.bucketValue.compareTo(o2.bucketValue));

      ArrayList<FacetBucket> out = new ArrayList<>(buckets.size());
      for (SortVal sv : lst) {
        out.add( sv.bucket );
      }
      out.addAll(nulls);
      sortedBuckets = out;
    }
    assert null != sortedBuckets;
  }

  boolean isBucketComplete(FacetBucket bucket, Context mcontext) {
    if (mcontext.numShards <= 1 || shardHasMoreBuckets==null) return true;
    for (int shard=0; shard < mcontext.numShards; shard++) {
      // bucket is incomplete if we didn't see the bucket for this shard, and the shard has more buckets
      if (!mcontext.getShardFlag(bucket.bucketNumber, shard) && shardHasMoreBuckets!=null && shardHasMoreBuckets.get(shard)) {
        return false;
      }
    }
    return true;
  }

  private boolean prunedBuckets = false;

  protected boolean prunedBuckets() {
    return prunedBuckets;
  }

  private void pruneBuckets(FacetRequest.FacetSort initial_sort) {
    assert !prunedBuckets; // check this condition externally; this clarifies calling logic, at expense of slight increase in verbosity
    sortBuckets(initial_sort);
    long first = freq.offset;
    long end = freq.limit >=0 ? first + (int) freq.limit : Integer.MAX_VALUE;
    long last = Math.min(sortedBuckets.size(), end);
    List<FacetBucket> prunedBuckets = new ArrayList<>(Math.max(0, (int)(last - first)));
    boolean refine = freq.refine != null && freq.refine != FacetRequest.RefineMethod.NONE;
    int off = (int)freq.offset;
    int lim = freq.limit >= 0 ? (int)freq.limit : Integer.MAX_VALUE;
    for (FacetBucket bucket : sortedBuckets) {
      if (bucket.getCount() < freq.mincount) {
        // prune all buckets not meeting mincount
        buckets.remove(bucket.bucketValue);
        continue;
      }
      if (refine && !isBucketComplete(bucket,mcontext)) {
        // prune all incomplete buckets
        buckets.remove(bucket.bucketValue);
        continue;
      }

      if (off > 0) {
        // prune all buckets before specified offset
        buckets.remove(bucket.bucketValue);
        --off;
        continue;
      }

      if (prunedBuckets.size() >= lim) {
        // prune all buckets after specified limit
        buckets.remove(bucket.bucketValue);
        continue;
      }

      prunedBuckets.add(bucket);
    }
    sortedBuckets = prunedBuckets;
    this.prunedBuckets = true;
  }

  private Collection<Object> initTopLevelSubs(int pass, FacetRequest.FacetSort initial_sort, final Map<Context.TopLevelSub, Collection<String>> topLevelSubs) {
    assert topLevelSubs != Context.NONE_ENTRY;
    final boolean initialTopLevelPivot = pass == passLimit;
    if (!prunedBuckets) {
      // only do this the first time
      assert initialTopLevelPivot : "pass="+pass+", passLimit="+passLimit;
      pruneBuckets(initial_sort);
    }
    return getTopLevelSkipBuckets(initialTopLevelPivot, topLevelSubs);
  }

  private Collection<Object> getTopLevelSkipBuckets(boolean initialTopLevelPivot, final Map<Context.TopLevelSub, Collection<String>> topLevelSubs) {
    ArrayList<Object> skipBuckets = new ArrayList<>(sortedBuckets.size());
    final Collection<String> childrenWithTopLevelDescendants = topLevelSubs.get(Context.TopLevelSub.DESCENDANT);
    final Collection<String> topLevelChildren = topLevelSubs.get(Context.TopLevelSub.CHILD);
    final int mapInitSize = (childrenWithTopLevelDescendants == null ? 0 : childrenWithTopLevelDescendants.size()) + (topLevelChildren == null ? 0 : topLevelChildren.size());
    for (FacetBucket bucket : sortedBuckets) {
      Map<String, Object> refinement = new HashMap<>(mapInitSize);
      if (childrenWithTopLevelDescendants != null) {
        bucket.getRefinement(mcontext, childrenWithTopLevelDescendants, refinement);
      }
      if (topLevelChildren != null) {
        if (initialTopLevelPivot) {
          assert topLevelChildrenAbsent(bucket, topLevelChildren);
          for (String key : topLevelChildren) {
            refinement.put(key, Collections.EMPTY_MAP);
          }
        } else {
          bucket.getRefinement(mcontext, topLevelChildren, refinement);
        }
      }
      if (!refinement.isEmpty()) {
        skipBuckets.add(Arrays.asList(bucket.bucketValue, refinement));
      }
    }
    return skipBuckets.isEmpty() ? null : skipBuckets;
  }

  private boolean topLevelChildrenAbsent(FacetBucket bucket, Collection<String> topLevelChildren) {
    if (bucket.subs == null) {
      return true;
    }
    for (String key : topLevelChildren) {
      if (bucket.subs.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  private int passLimit = -1;

  @Override
  public Map<String, Object> getRefinement(Context mcontext) {
    // step 1) If this facet request has refining, then we need to fully request top buckets that were not seen by this shard.
    // step 2) If this facet does not have refining, but some sub-facets do, we need to check/recurse those sub-facets in *every* top bucket.
    // A combination of the two is possible and makes step 2 redundant for any buckets we fully requested in step 1.

    final FacetRequest.FacetSort initial_sort = null == freq.prelim_sort ? freq.sort : freq.prelim_sort;

    Map<Context.TopLevelSub, Collection<String>> topLevelSubs = null;
    boolean overlapTopLevelSubsWithParentRefinement = false;

    if (passLimit == -1) {
      // `passLimit` always the same wrt first time `getRefinement(...)` is called, regardless of `freq.refine`
      passLimit = mcontext.getPass() + 1;
      if (freq.refine != FacetRequest.RefineMethod.SIMPLE) {
        if ((topLevelSubs = mcontext.hasTopLevelSubs(freq)) != Context.NONE_ENTRY) {
          // TODO: NO! If you don't prune buckets at this level, you lose one of the main benefits
          //  of having a subfacet be `topLevel` -- and you _can't_ prune buckets while parent refinement
          //  is ongoing (see below).
          overlapTopLevelSubsWithParentRefinement = true;
        }
        if (!prunedBuckets) {
          // nocommit: we can't do this!!
          // gradlew :solr:core:test --tests "org.apache.solr.search.facet.TestCloudJSONFacetJoinDomain.testRandom" -Ptests.jvms=4
          //     -Ptests.jvmargs=-XX:TieredStopAtLevel=1 -Ptests.seed=A4C143860EF04282 -Ptests.file.encoding=US-ASCII
          // TODO: NO! you cannot prune buckets while parental refinement is ongoing, because there could be
          //  shards with _leaf_ buckets at the parent level that can contribute new counts (affecting sort, and
          //  therefore pruning), and even contribute entirely new values (in phase#2, even if the child facet has
          //  specified `refine:NONE`)
          pruneBuckets(initial_sort);
        }
      }
    } else if (mcontext.getPass() >= passLimit) {
      topLevelSubs = mcontext.hasTopLevelSubs(freq);
      if (topLevelSubs == Context.NONE_ENTRY) {
        return null;
      } else {
        Collection<Object> initTopLevelSubs = initTopLevelSubs(mcontext.getPass(), initial_sort, topLevelSubs);
        return initTopLevelSubs == null ? null : Collections.singletonMap("_s", initTopLevelSubs);
      }
    }

    Map<String,Object> refinement = null;

    Collection<String> tags = mcontext.getSubsWithRefinement(freq);
    if (tags.isEmpty() && !freq.doRefine()) {
      // we don't have refining, and neither do our subs
      if (!overlapTopLevelSubsWithParentRefinement) {
        return null;
      } else {
        Collection<Object> skipBuckets = getTopLevelSkipBuckets(true, topLevelSubs);
        return skipBuckets.isEmpty() ? null : Collections.singletonMap("_s", skipBuckets);
      }
    }

    // Tags for sub facets that have partial facets somewhere in their children.
    // If we are missing a bucket for this shard, we'll need to get the specific buckets that need refining.
    Collection<String> tagsWithPartial = mcontext.getSubsWithPartial(freq);

    boolean thisMissing = mcontext.bucketWasMissing(); // Was this whole facet missing (i.e. inside a bucket that was missing)?
    boolean shardHasMore = shardHasMoreBuckets != null && shardHasMoreBuckets.get(mcontext.shardNum);  // shard indicated it has more buckets
    shardHasMore |= thisMissing;  // if we didn't hear from the shard at all, assume it as more buckets

    // If we know we've seen all the buckets from a shard, then we don't have to add to leafBuckets or partialBuckets, only skipBuckets
    boolean isCommandPartial = freq.returnsPartial() || freq.processEmpty; // TODO: should returnsPartial() check processEmpty internally?
    boolean returnedAllBuckets = !shardHasMore && !freq.processEmpty;  // did the shard return all of the possible buckets at this level? (pretend it didn't if processEmpty is set)

    if (returnedAllBuckets && tags.isEmpty() && tagsWithPartial.isEmpty()) {
      // this shard returned all of its possible buckets, and there were no sub-facets with partial results
      // or sub-facets that require refining
      if (!overlapTopLevelSubsWithParentRefinement) {
        return null;
      } else {
        Collection<Object> skipBuckets = getTopLevelSkipBuckets(true, topLevelSubs);
        return skipBuckets.isEmpty() ? null : Collections.singletonMap("_s", skipBuckets);
      }
    }

    long numBucketsToCheck = Integer.MAX_VALUE; // use max-int instead of max-long to avoid overflow
    if (freq.limit >= 0) {
      numBucketsToCheck = freq.offset + freq.limit; // effective limit
      if (-1 == freq.overrefine) { // DEFAULT: use heuristic for overrefinement

        // when we don't have to worry about mincount pruning, there is no need for any
        // over refinement for these sorts..
        if (freq.mincount <= 1 && ("index".equals(initial_sort.sortVariable)
                                   || ("count".equals(initial_sort.sortVariable)
                                       && FacetRequest.SortDirection.desc == initial_sort.sortDirection))) {
          // No-Op
        } else if (0 <= freq.overrequest) {
          // if user asked for an explicit amount of overrequesting,
          // (but did not provide an explicit amount of overrefinement)
          // then use the same amount for overrefinement
          numBucketsToCheck += freq.overrequest;
        } else {
          // default: add 10% plus 4 
          numBucketsToCheck = (long) (numBucketsToCheck * 1.1 +4); 
        }

        // TODO: should we scale our 'overrefine' (heuristic) value based on 'mincount' ?
        //
        // If mincount=M > 1 should we be doing something like numBucketsToCheck *= M ?
        // Perhaps that would make more sense in the 'overrequest' heuristic calc?
        //
        // Maybe we should look at how many buckets were fully populated in phase#1 AND
        // already meet the 'mincount', and use the the difference between that number
        // and 'limit' to decide a scaling factor for 'overrefine' ?
        
      } else { // user requested an explicit amount of overrefinement
        numBucketsToCheck += freq.overrefine;
      }
    }
    numBucketsToCheck = Math.min(buckets.size(), numBucketsToCheck);

    Collection<FacetBucket> bucketList;
    if (buckets.size() < numBucketsToCheck) {
      if (overlapTopLevelSubsWithParentRefinement) {
        // we already sorted these (and pruned)
        bucketList = sortedBuckets;
      } else {
        // no need to sort (yet)
        // todo: but we may need to filter.... simplify by always sorting?
        bucketList = buckets.values();
      }
    } else {
      // don't re-sort (the prerefinement values) if our subclass already did it
      if (sortedBuckets == null) {
        sortBuckets(initial_sort);  // todo: make sure this filters buckets as well
      }
      bucketList = sortedBuckets;
    }

    ArrayList<Object> leafBuckets = null;    // "_l" missing buckets specified by bucket value only (no need to specify anything further)
    ArrayList<Object> partialBuckets = null; // "_p" missing buckets that have a partial sub-facet that need to specify those bucket values... each entry is [bucketval, subs]
    ArrayList<Object> skipBuckets = null;    // "_s" present buckets that we need to recurse into because children facets have refinement requirements. each entry is [bucketval, subs]

    Collection<String> childrenWithTopLevelDescendants = null;
    Collection<String> topLevelChildren = null;
    int mapInitSize = 0;
    if (overlapTopLevelSubsWithParentRefinement) {
      childrenWithTopLevelDescendants = topLevelSubs.get(Context.TopLevelSub.DESCENDANT);
      topLevelChildren = topLevelSubs.get(Context.TopLevelSub.CHILD);
      mapInitSize = (childrenWithTopLevelDescendants == null ? 0 : childrenWithTopLevelDescendants.size()) + (topLevelChildren == null ? 0 : topLevelChildren.size());
    }
    for (FacetBucket bucket : bucketList) {
      Map<String,Object> bucketRefinement = null;
      if (numBucketsToCheck-- <= 0) break;
      // if this bucket is missing,
      assert thisMissing == false || thisMissing == true && mcontext.getShardFlag(bucket.bucketNumber) == false;
      boolean saw = !thisMissing && mcontext.getShardFlag(bucket.bucketNumber);
      if (!saw && !returnedAllBuckets) {
        // we didn't see the bucket for this shard, and it's possible that the shard has it

        // find facets that we need to fill in buckets for
        if (!tagsWithPartial.isEmpty()) {
          boolean prev = mcontext.setBucketWasMissing(true);
          bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
          mcontext.setBucketWasMissing(prev);

          if (bucketRefinement != null) {
            if (partialBuckets==null) partialBuckets = new ArrayList<>();
            partialBuckets.add( Arrays.asList(bucket.bucketValue, bucketRefinement) );
          }
        }

        // if we didn't add to "_p" (missing with partial sub-facets), then we should add to "_l" (missing leaf)
        // If overlapTopLevelSubs, then topLevel descendants exist and have no refinement; these are guaranteed
        // to be represented to skipBuckets by topLevel fallback
        if (bucketRefinement == null && !overlapTopLevelSubsWithParentRefinement) {
          if (leafBuckets == null) leafBuckets = new ArrayList<>();
          leafBuckets.add(bucket.bucketValue);
        }

      } else if (!tags.isEmpty()) {
        // we had this bucket, but we need to recurse to certain children that have refinements
        bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
        if (bucketRefinement != null) {
          if (skipBuckets == null) skipBuckets = new ArrayList<>();
          skipBuckets.add( Arrays.asList(bucket.bucketValue, bucketRefinement) );
        }
      }

      if (overlapTopLevelSubsWithParentRefinement) {
        // ensure that _all_ buckets with topLevel descendants are represented in the refinement request
        final Collection<String> filteredChildrenWithTopLevelDescendants;
        if (bucketRefinement != null) {
          final Map<String, Object> topLevelRefinement = bucketRefinement; // to use in predicate below
          filteredChildrenWithTopLevelDescendants = childrenWithTopLevelDescendants == null ? null : childrenWithTopLevelDescendants
                  .stream().filter((tag) -> !topLevelRefinement.containsKey(tag)).collect(Collectors.toList());
        } else {
          bucketRefinement = new HashMap<>(mapInitSize);
          filteredChildrenWithTopLevelDescendants = childrenWithTopLevelDescendants;
        }
        if (filteredChildrenWithTopLevelDescendants != null && !filteredChildrenWithTopLevelDescendants.isEmpty()) {
          bucket.getRefinement(mcontext, filteredChildrenWithTopLevelDescendants, bucketRefinement);
        }
        if (topLevelChildren != null) {
          assert topLevelChildrenAbsent(bucket, topLevelChildren);
          for (String key : topLevelChildren) {
            bucketRefinement.put(key, Collections.EMPTY_MAP);
          }
        }
        if (!bucketRefinement.isEmpty()) {
          if (skipBuckets == null) skipBuckets = new ArrayList<>();
          skipBuckets.add( Arrays.asList(bucket.bucketValue, bucketRefinement) );
        }
      }
    }

    // TODO: what if we don't need to refine any variable buckets, but we do need to contribute to numBuckets, missing, allBuckets, etc...
    // because we were "partial".  That will be handled at a higher level (i.e. we'll be in someone's missing bucket?)
    // TODO: test with a sub-facet with a limit of 0 and something like a missing bucket
    if (leafBuckets != null || partialBuckets != null || skipBuckets != null) {
      refinement = new HashMap<>(3);
      if (leafBuckets != null) refinement.put("_l",leafBuckets);
      if (partialBuckets != null) refinement.put("_p", partialBuckets);
      if (skipBuckets != null) refinement.put("_s", skipBuckets);
    }

    refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial);

    return refinement;
  }

  // utility method for subclasses to override to finish calculating faceting (special buckets in field facets)... this feels hacky and we
  // should find a better way.
  Map<String,Object> getRefinementSpecial(Context mcontext, Map<String,Object> refinement, Collection<String> tagsWithPartial) {
    return refinement;
  }


}
