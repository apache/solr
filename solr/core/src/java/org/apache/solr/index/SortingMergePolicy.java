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
package org.apache.solr.index;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.Sort;

/**
 * A {@link MergePolicy} that does no merging of its own and only carries a {@link Sort} to the
 * index writer.
 *
 * @deprecated Configure the index sort directly with {@code <indexSort>} in {@code <indexConfig>}
 *     instead of wrapping a merge policy to carry a sort.
 */
@Deprecated
public final class SortingMergePolicy extends FilterMergePolicy {

  private final Sort sort;

  /** Create a new {@code MergePolicy} that sorts documents with the given {@code sort}. */
  public SortingMergePolicy(MergePolicy in, Sort sort) {
    super(in);
    this.sort = sort;
  }

  /** Return the {@link Sort} order that is used to sort segments when merging. */
  public Sort getSort() {
    return sort;
  }

  @Override
  public String toString() {
    return "SortingMergePolicy(" + in + ", sort=" + sort + ")";
  }
}
