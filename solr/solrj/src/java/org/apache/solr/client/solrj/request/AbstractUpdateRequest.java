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
package org.apache.solr.client.solrj.request;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;

public abstract class AbstractUpdateRequest extends CollectionRequiringSolrRequest<UpdateResponse> {
  protected ModifiableSolrParams params = new ModifiableSolrParams(); // maybe make final; no setter
  protected int commitWithin = -1;

  public AbstractUpdateRequest(METHOD m, String path) {
    super(m, path, SolrRequestType.UPDATE);
  }

  public enum CommitOption {
    /** Makes the changes visible. True by default. */
    openSearcher,
    /** The request should wait for the changes to be visible. True by default. */
    waitSearcher,
    /** A faster commit that's slightly less durable, and only affects the leader replica. */
    softCommit;

    /** Default standard set of commit options: waitSearcher and openSearcher */
    public static final EnumSet<CommitOption> DEFAULTS = EnumSet.of(waitSearcher, openSearcher);
  }

  @Deprecated
  public enum ACTION {
    COMMIT,
    OPTIMIZE
  }

  /**
   * Solr returns early but schedules a (typically soft) commit. When Solr finishes processing the
   * changes, it will then schedule the next auto soft-commit to occur no later than the provided
   * time interval. Other commit requests and automatic ones may expedite that commit to occur
   * sooner.
   */
  public AbstractUpdateRequest commitWithin(int val, TimeUnit unit) {
    this.commitWithin = Math.toIntExact(unit.toMillis(val));
    return this;
  }

  public AbstractUpdateRequest commit() {
    params.set(UpdateParams.COMMIT, true);
    return this;
  }

  /** Commits with additional options. The absense of an option means to disable the option. */
  public AbstractUpdateRequest commit(EnumSet<CommitOption> options) {
    params.set(UpdateParams.COMMIT, true);
    // only set if varies from default
    if (options.contains(CommitOption.softCommit) == true) {
      params.set(UpdateParams.SOFT_COMMIT, true);
    }
    if (options.contains(CommitOption.openSearcher) == false) {
      params.set(UpdateParams.OPEN_SEARCHER, false);
      // and don't bother considering waitSearcher as it's irrelevant
    } else if (options.contains(CommitOption.waitSearcher) == false) {
      params.set(UpdateParams.WAIT_SEARCHER, false);
    }
    return this;
  }

  public AbstractUpdateRequest commitAndOptimize(EnumSet<CommitOption> options, int maxSegments) {
    params.set(UpdateParams.OPTIMIZE, true);
    params.set(UpdateParams.MAX_OPTIMIZE_SEGMENTS, maxSegments);
    return commit(options);
  }

  public AbstractUpdateRequest commitAndExpungeDeletes(EnumSet<CommitOption> options) {
    params.set(UpdateParams.EXPUNGE_DELETES, true);
    return commit(options);
  }

  /** Sets appropriate parameters for the given ACTION */
  @Deprecated
  public AbstractUpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher) {
    return setAction(action, waitFlush, waitSearcher, 1);
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action, boolean waitFlush, boolean waitSearcher, boolean softCommit) {
    return setAction(action, waitFlush, waitSearcher, softCommit, 1);
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments) {
    return setAction(action, waitFlush, waitSearcher, false, maxSegments);
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action, boolean waitFlush, boolean waitSearcher, boolean softCommit, int maxSegments) {
    if (action == ACTION.OPTIMIZE) {
      params.set(UpdateParams.OPTIMIZE, "true");
      params.set(UpdateParams.MAX_OPTIMIZE_SEGMENTS, maxSegments);
    } else if (action == ACTION.COMMIT) {
      params.set(UpdateParams.COMMIT, "true");
      params.set(UpdateParams.SOFT_COMMIT, String.valueOf(softCommit));
    }
    params.set(UpdateParams.WAIT_SEARCHER, String.valueOf(waitSearcher));
    return this;
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action,
      boolean waitFlush,
      boolean waitSearcher,
      int maxSegments,
      boolean softCommit,
      boolean expungeDeletes) {
    setAction(action, waitFlush, waitSearcher, softCommit, maxSegments);
    params.set(UpdateParams.EXPUNGE_DELETES, String.valueOf(expungeDeletes));
    return this;
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action,
      boolean waitFlush,
      boolean waitSearcher,
      int maxSegments,
      boolean expungeDeletes) {
    return setAction(action, waitFlush, waitSearcher, maxSegments, false, expungeDeletes);
  }

  @Deprecated
  public AbstractUpdateRequest setAction(
      ACTION action,
      boolean waitFlush,
      boolean waitSearcher,
      int maxSegments,
      boolean softCommit,
      boolean expungeDeletes,
      boolean openSearcher) {
    setAction(action, waitFlush, waitSearcher, maxSegments, softCommit, expungeDeletes);
    params.set(UpdateParams.OPEN_SEARCHER, String.valueOf(openSearcher));
    return this;
  }

  /**
   * @since Solr 1.4
   */
  public AbstractUpdateRequest rollback() {
    params.set(UpdateParams.ROLLBACK, "true");
    return this;
  }

  public void setParam(String param, String value) {
    params.set(param, value);
  }

  /** Sets the parameters for this update request, overwriting any previous */
  public void setParams(ModifiableSolrParams params) {
    this.params = Objects.requireNonNull(params);
  }

  @Override
  public ModifiableSolrParams getParams() {
    return params;
  }

  @Override
  protected UpdateResponse createResponse(NamedList<Object> namedList) {
    return new UpdateResponse();
  }

  public boolean isWaitSearcher() {
    return params.getBool(UpdateParams.WAIT_SEARCHER, false);
  }

  public ACTION getAction() {
    if (params.getBool(UpdateParams.COMMIT, false)) return ACTION.COMMIT;
    if (params.getBool(UpdateParams.OPTIMIZE, false)) return ACTION.OPTIMIZE;
    return null;
  }

  @Deprecated
  public void setWaitSearcher(boolean waitSearcher) {
    setParam(UpdateParams.WAIT_SEARCHER, waitSearcher + "");
  }

  public int getCommitWithin() {
    return commitWithin;
  }

  @Deprecated
  public AbstractUpdateRequest setCommitWithin(int commitWithin) {
    this.commitWithin = commitWithin;
    return this;
  }
}
