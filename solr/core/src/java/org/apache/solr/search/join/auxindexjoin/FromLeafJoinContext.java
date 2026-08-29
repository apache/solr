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
package org.apache.solr.search.join.auxindexjoin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FromLeafJoinContext {
  final JoinIndexUtils.CacheAndCount matches;
  final ForeignKeyColumn fkColumn;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public FromLeafJoinContext(JoinIndexUtils.CacheAndCount matches, ForeignKeyColumn fkColumn) {
    this.matches = matches;
    this.fkColumn = fkColumn;
  }

  static @NonNull FromLeafJoinContext heavyLoadFromLeaf(
      Weight fromWeight, String fromField, LeafReaderContext ctx, boolean loadFkColumn) {
    try {
      JoinIndexUtils.CacheAndCount docset = JoinIndexUtils.computeDocIdSet(fromWeight, ctx);
      boolean loadFk = docset != null && docset.count() > 0 && loadFkColumn;
      if (JoinIndexUtils.diagnosticsEnabled(log)) {
        JoinIndexUtils.logDiagnostic(
            log,
            "AUXIJOIN evt=fromLeaf fromSeg={} ord={} fromMatches={} missingPair={}"
                + " fkLoaded={}",
            JoinIndexUtils.segmentName(ctx),
            ctx.ord,
            docset == null ? -1 : docset.count(),
            loadFkColumn,
            loadFk);
      }
      if (loadFk) {
        // waste case: noone to-seg read FK,
        return new FromLeafJoinContext(docset, new ForeignKeyColumn(ctx, fromField));
      } else {
        return new FromLeafJoinContext(docset, null);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
