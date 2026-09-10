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
package org.apache.solr.common.params;

/**
 * A collection of standard params used by Update handlers
 *
 * @since solr 1.2
 */
public interface UpdateParams {

  /** Open up a new searcher as part of a commit */
  String OPEN_SEARCHER = "openSearcher";

  /** wait for the searcher to be registered/visible */
  String WAIT_SEARCHER = "waitSearcher";

  String SOFT_COMMIT = "softCommit";

  /** overwrite indexing fields */
  String OVERWRITE = "overwrite";

  /** Commit everything after the command completes */
  String COMMIT = "commit";

  /** Commit within a certain time period (in ms) */
  String COMMIT_WITHIN = "commitWithin";

  /** Optimize the index and commit everything after the command completes */
  String OPTIMIZE = "optimize";

  /** expert: calls IndexWriter.prepareCommit */
  String PREPARE_COMMIT = "prepareCommit";

  /** Fail a commit when the core or collection is in read-only mode */
  String FAIL_ON_READ_ONLY = "failOnReadOnly";

  /** Rollback update commands */
  String ROLLBACK = "rollback";

  String COLLECTION = "collection";

  /**
   * Select the update processor chain to use. A RequestHandler may or may not respect this
   * parameter
   */
  String UPDATE_CHAIN = "update.chain";

  /** Override the content type used for UpdateLoader * */
  String ASSUME_CONTENT_TYPE = "update.contentType";

  /**
   * If optimizing, set the maximum number of segments left in the index after optimization.
   * Integer.MAX_INT is the default to respect maxMergeSegmentsMB
   */
  String MAX_OPTIMIZE_SEGMENTS = "maxSegments";

  String EXPUNGE_DELETES = "expungeDeletes";

  /** Return versions of updates? */
  String VERSIONS = "versions";

  /**
   * If set to true, then Solr must fail to process any Atomic Update which can not be done
   * "In-Place" without re-indexing the entire document.
   */
  String REQUIRE_PARTIAL_DOC_UPDATES_INPLACE = "update.partial.requireInPlace";
}
