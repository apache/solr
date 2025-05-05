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

import java.util.Map;
import org.apache.solr.request.SolrQueryRequest;

/** A commit index command encapsulated in an object. */
public class CommitUpdateCommand extends UpdateCommand {

  // Behavior about opening a searcher after a hard commit.
  // - Open a standard searcher and wait for it. (default)
  //   openSearcher == true && waitSearcher == true && closeSearcher == false
  // - Open a standard searcher, but do not wait for it.
  //   openSearcher == true && waitSearcher == false && closeSearcher == false
  // - Open a real-time searcher quicker without auto-warming.
  //   openSearcher == false && closeSearcher == false
  // - Do not open any searcher and prevent further updates (e.g. when the core is closing).
  //   closeSearcher == true

  public boolean optimize;
  public boolean openSearcher = true; // open a new searcher as part of a hard commit
  public boolean waitSearcher = true;
  private boolean closeSearcher = false;
  public boolean expungeDeletes = false;
  public boolean softCommit = false;
  public boolean prepareCommit = false;

  /**
   * User provided commit data. Can be let to null if there is none. It is possible to commit this
   * user data, even if there is no uncommitted change in the index writer, provided this user data
   * is not empty.
   */
  public Map<String, String> commitData;

  /**
   * During optimize, optimize down to &lt;= this many segments. Must be &gt;= 1
   *
   * @see org.apache.lucene.index.IndexWriter#forceMerge(int)
   */
  public int maxOptimizeSegments = Integer.MAX_VALUE; // So we respect MaxMergeSegmentsMB by default

  public CommitUpdateCommand(SolrQueryRequest req, boolean optimize) {
    super(req);
    this.optimize = optimize;
  }

  /**
   * Creates a {@link CommitUpdateCommand} to commit before closing the core and also prevent any
   * new update by setting the core in read-only mode. Does not open a new searcher.
   */
  public static CommitUpdateCommand closeOnCommit(SolrQueryRequest req, boolean optimize) {
    CommitUpdateCommand cmd = new CommitUpdateCommand(req, optimize);
    cmd.openSearcher = false;
    cmd.waitSearcher = false;
    cmd.closeSearcher = true;
    return cmd;
  }

  /**
   * Indicates whether this command is a commit before the core is closed. Any new updates must be
   * prevented after the commit.
   */
  public boolean isClosingOnCommit() {
    return closeSearcher;
  }

  @Override
  public String name() {
    return "commit";
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder(super.toString())
            .append(",optimize=")
            .append(optimize)
            .append(",openSearcher=")
            .append(openSearcher)
            .append(",expungeDeletes=")
            .append(expungeDeletes)
            .append(",softCommit=")
            .append(softCommit)
            .append(",prepareCommit=")
            .append(prepareCommit);
    if (commitData != null) {
      sb.append(",commitData=").append(commitData);
    }
    sb.append('}');
    return sb.toString();
  }
}
