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

/**
 * The supported orderings for the {@code <segmentSort>} index configuration, which controls the
 * order in which a {@link org.apache.lucene.index.DirectoryReader}'s leaf readers (segments) are
 * visited at search time. This is "between-segment" ordering, distinct from Lucene's index sort
 * (the "within-segment" order of documents), and has no effect on merging.
 */
public enum SegmentSort {
  /** Do not impose any ordering on segments (Lucene's default). */
  NONE,
  /** Visit older segments (by creation timestamp) first. */
  TIME_ASC,
  /** Visit more recently created segments (by creation timestamp) first. */
  TIME_DESC
}
