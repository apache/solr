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

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

/**
 * Related to the {@code _version_} field, in connection with the {@link UpdateLog}.
 *
 * @lucene.internal
 */
public class VersionInfo {

  private final UpdateLog ulog;

  private final SchemaField versionField;

  /**
   * Gets and returns the {@link org.apache.solr.common.params.CommonParams#VERSION_FIELD} from the
   * specified schema, after verifying that it is indexed, stored, and single-valued. If any of
   * these pre-conditions are not met, it throws a SolrException with a user suitable message
   * indicating the problem.
   */
  public static SchemaField getAndCheckVersionField(IndexSchema schema) throws SolrException {
    final String errPrefix =
        VERSION_FIELD
            + " field must exist in schema and be searchable (indexed or docValues) and retrievable(stored or docValues) and not multiValued";
    SchemaField sf = schema.getFieldOrNull(VERSION_FIELD);

    if (null == sf) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          errPrefix + " (" + VERSION_FIELD + " does not exist)");
    }
    if (!sf.indexed() && !sf.hasDocValues()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          errPrefix + " (" + VERSION_FIELD + " not searchable");
    }
    if (!sf.stored() && !sf.hasDocValues()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          errPrefix + " (" + VERSION_FIELD + " not retrievable");
    }
    if (sf.multiValued()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          errPrefix + " (" + VERSION_FIELD + " is multiValued");
    }

    return sf;
  }

  public VersionInfo(UpdateLog ulog) {
    this.ulog = ulog;
    IndexSchema schema = ulog.uhandler.core.getLatestSchema();
    versionField = getAndCheckVersionField(schema);
  }

  public SchemaField getVersionField() {
    return versionField;
  }

  /*
  // todo: initialize... use current time to start?
  // a clock that increments by 1 for every operation makes it easier to detect missing
  // messages, but raises other issues:
  // - need to initialize to largest thing in index or tlog
  // - when becoming leader, need to make sure it's greater than
  // - using to detect missing messages means we need to keep track per-leader, or make
  //   sure a new leader starts off with 1 greater than the last leader.
  private final AtomicLong clock = new AtomicLong();

  public long getNewClock() {
    return clock.incrementAndGet();
  }

  // Named *old* to prevent accidental calling getClock and expecting a new updated clock.
  public long getOldClock() {
    return clock.get();
  }
  */

  /**
   * We are currently using this time-based clock to avoid going back in time on a server restart
   * (i.e. we don't want version numbers to start at 1 again).
   */

  // Time-based lamport clock.  Good for introducing some reality into clocks (to the degree
  // that times are somewhat synchronized in the cluster).
  // Good if we want to relax some constraints to scale down to where only one node may be
  // up at a time.  Possibly harder to detect missing messages (because versions are not
  // contiguous).
  private long vclock;

  private final Object clockSync = new Object();

  @SuppressForbidden(
      reason =
          "need currentTimeMillis just for getting realistic version stamps, does not assume monotonicity")
  public long getNewClock() {
    synchronized (clockSync) {
      long time = System.currentTimeMillis();
      long result = time << 20;
      if (result <= vclock) {
        result = vclock + 1;
      }
      vclock = result;
      return vclock;
    }
  }

  public Long lookupVersion(BytesRef idBytes) {
    return ulog.lookupVersion(idBytes);
  }

  /**
   * Returns the latest version from the index, searched by the given id (bytes) as seen from the
   * realtime searcher. Returns null if no document can be found in the index for the given id.
   */
  @SuppressWarnings({"unchecked"})
  public Long getVersionFromIndex(BytesRef idBytes) {
    // TODO: we could cache much of this and invalidate during a commit.
    // TODO: most DocValues classes are threadsafe - expose which.

    RefCounted<SolrIndexSearcher> newestSearcher = ulog.uhandler.core.getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = newestSearcher.get();
      long lookup = searcher.lookupId(idBytes);
      if (lookup < 0) return null; // this means the doc doesn't exist in the index yet

      ValueSource vs = versionField.getType().getValueSource(versionField, null);
      Map<Object, Object> context = ValueSource.newContext(searcher);
      vs.createWeight(context, searcher);
      FunctionValues fv =
          vs.getValues(context, searcher.getTopReaderContext().leaves().get((int) (lookup >> 32)));
      long ver = fv.longVal((int) lookup);
      return ver;

    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error reading version from index", e);
    } finally {
      if (newestSearcher != null) {
        newestSearcher.decref();
      }
    }
  }
}
