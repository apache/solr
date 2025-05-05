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
package org.apache.solr.response.transform;

import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.util.Version;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;

/**
 * @since solr 4.0
 */
public class ShardAugmenterFactory extends TransformerFactory {
  protected enum Style {
    URLS,
    ID;

    public static Optional<Style> getStyle(final String s) {
      if (null == s || s.trim().isEmpty()) {
        return Optional.empty();
      }
      try {
        return Optional.of(Style.valueOf(s.toUpperCase(Locale.ROOT)));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown [shard] style: " + s);
      }
    }
  }

  protected Optional<Style> configuredDefaultStyle = Optional.empty();

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    if (defaultUserArgs != null) {
      configuredDefaultStyle = Style.getStyle(defaultUserArgs);
    }
  }

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {

    final SolrCore core = req.getCore();
    if (!core.getCoreContainer().isZooKeeperAware()) {
      return new ValueAugmenterFactory.ValueAugmenter(field, "[not a shard request]");
    }

    final CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
    if (null == cd) { // Not sure if these can even happen?
      return new ValueAugmenterFactory.ValueAugmenter(field, "[unknown]");
    }

    final Style style =
        Style.getStyle(params.get("style", ""))
            .or(() -> configuredDefaultStyle)
            .orElse( // if not cnfigured at init, default Style driven by luceneMatchVersion
                core.getSolrConfig().luceneMatchVersion.onOrAfter(Version.LUCENE_9_5_0)
                    ? Style.ID
                    : Style.URLS);

    final String shardId = cd.getShardId();
    if (style == Style.ID) {
      return new ValueAugmenterFactory.ValueAugmenter(field, shardId);
    }

    final Slice slice =
        req.getCore()
            .getCoreContainer()
            .getZkController()
            .getClusterState()
            .getCollection(cd.getCollectionName())
            .getSlice(shardId);
    final String urls =
        slice.getReplicas().stream().map(Replica::getCoreUrl).collect(Collectors.joining("|"));
    return new ValueAugmenterFactory.ValueAugmenter(field, urls);
  }
}
