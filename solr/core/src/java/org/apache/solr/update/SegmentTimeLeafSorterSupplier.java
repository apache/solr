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

package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SegmentTimeLeafSorterSupplier implements LeafSorterSupplier {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String TIME_FIELD = "timestamp";
  private static final SegmentSort DEFAULT_SORT_OPTIONS = SegmentSort.NONE;

  private SegmentSort sortOptions;
  private Comparator<LeafReader> leafSorter;

  public SegmentTimeLeafSorterSupplier() {
    this(DEFAULT_SORT_OPTIONS);
  }

  public SegmentTimeLeafSorterSupplier(SegmentSort sortOptions) {
    this.sortOptions = sortOptions;
  }

  @Override
  public Comparator<LeafReader> getLeafSorter() {
    if (leafSorter == null) {
      if (SegmentSort.NONE == sortOptions) {
        return null;
      }
      boolean ascSort = SegmentSort.TIME_ASC == sortOptions;
      long missingValue = ascSort ? Long.MAX_VALUE : Long.MIN_VALUE;
      leafSorter =
          Comparator.comparingLong(
              r -> {
                try {
                  return Long.parseLong(
                      ((SegmentReader) r).getSegmentInfo().info.getDiagnostics().get(TIME_FIELD));
                } catch (Exception e) {
                  log.error("Error getting time stamp for SegmentReader", e);
                  return missingValue;
                }
              });
      return ascSort ? leafSorter : leafSorter.reversed();
    }
    ;
    return leafSorter;
  }

  public SegmentSort getSortOptions() {
    return sortOptions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(50);
    sb.append("SegmentTimeLeafSorter{").append(sortOptions).append('}');
    return sb.toString();
  }
}
