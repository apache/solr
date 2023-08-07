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
package org.apache.solr.search.join;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a query-time join query with scoring. It just calls {@link
 * JoinUtil#createJoinQuery(String, boolean, String, Query, org.apache.lucene.search.IndexSearcher,
 * ScoreMode)}. It runs subordinate query and collects values of "from" field and scores, then it
 * lookups these collected values in "to" field, and yields aggregated scores. Local parameters are
 * similar to {@link JoinQParserPlugin} <a
 * href="https://solr.apache.org/guide/solr/latest/query-guide/join-query-parser.html">{!join}</a>
 * This plugin doesn't have its own name, and is called by specifying local parameter <code>
 * {!join score=...}...</code>. Note: this parser is invoked even if you specify <code>score=none
 * </code>. <br>
 * Example:<code>q={!join from=manu_id_s to=id score=total}foo</code>
 *
 * <ul>
 *   <li>from - "foreign key" field name to collect values while enumerating subordinate query
 *       (denoted as <code>foo</code> in example above). it's better to have this field declared as
 *       <code>type="string" docValues="true"</code>. note: if <a
 *       href="https://solr.apache.org/guide/solr/latest/indexing-guide/docvalues.html">docValues</a>
 *       are not enabled for this field, it will work anyway, but it costs some memory for {@link
 *       UninvertingReader}. Also, numeric doc values are not supported until <a
 *       href="https://issues.apache.org/jira/browse/LUCENE-5868">LUCENE-5868</a>. Thus, it only
 *       supports {@link DocValuesType#SORTED}, {@link DocValuesType#SORTED_SET}, {@link
 *       DocValuesType#BINARY}.
 *   <li>fromIndex - optional parameter, a core name where subordinate query should run (and <code>
 *       from</code> values are collected) rather than current core. <br>
 *       Example:<code>q={!join from=manu_id_s to=id score=total fromIndex=products}foo</code>
 *   <li>to - "primary key" field name which is searched for values collected from subordinate
 *       query. it should be declared as <code>indexed="true"</code>. Now it's treated as a single
 *       value field.
 *   <li>score - one of {@link ScoreMode}: <code>none,avg,total,max,min</code>. Capital case is also
 *       accepted.
 * </ul>
 */
public class ScoreJoinQParserPlugin extends QParserPlugin {

  public static final String USE_CROSSCOLLECTION =
      "SolrCloud join: To join with a collection that might not be co-located, use method=crossCollection.";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SCORE = "score";
  public static final String CHECK_ROUTER_FIELD = "checkRouterField";

  static class OtherCoreJoinQuery extends SameCoreJoinQuery {
    private final String fromIndex;
    private final long fromCoreOpenTime;

    public OtherCoreJoinQuery(
        Query fromQuery,
        String fromField,
        String fromIndex,
        long fromCoreOpenTime,
        ScoreMode scoreMode,
        String toField) {
      super(fromQuery, fromField, toField, scoreMode);
      this.fromIndex = fromIndex;
      this.fromCoreOpenTime = fromCoreOpenTime;
    }

    @Override
    public Weight createWeight(
        IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
        throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();

      CoreContainer container = info.getReq().getCoreContainer();

      final SolrCore fromCore = container.getCore(fromIndex);

      if (fromCore == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
      }
      RefCounted<SolrIndexSearcher> fromHolder = fromCore.getRegisteredSearcher();
      final Query joinQuery;
      try {
        joinQuery =
            JoinUtil.createJoinQuery(
                fromField, true, toField, fromQuery, fromHolder.get(), this.scoreMode);
      } finally {
        fromCore.close();
        fromHolder.decref();
      }
      return joinQuery.rewrite(searcher.getIndexReader()).createWeight(searcher, scoreMode, boost);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (fromCoreOpenTime ^ (fromCoreOpenTime >>> 32));
      result = prime * result + ((fromIndex == null) ? 0 : fromIndex.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (!(obj instanceof OtherCoreJoinQuery)) return false;
      OtherCoreJoinQuery other = (OtherCoreJoinQuery) obj;
      return (fromCoreOpenTime == other.fromCoreOpenTime)
          && Objects.equals(fromIndex, other.fromIndex);
    }

    @Override
    public String toString(String field) {
      return "OtherCoreJoinQuery [fromIndex="
          + fromIndex
          + ", fromCoreOpenTime="
          + fromCoreOpenTime
          + " extends "
          + super.toString(field)
          + "]";
    }
  }

  static class SameCoreJoinQuery extends Query {
    protected final Query fromQuery;
    protected final ScoreMode scoreMode;
    protected final String fromField;
    protected final String toField;

    SameCoreJoinQuery(Query fromQuery, String fromField, String toField, ScoreMode scoreMode) {
      this.fromQuery = fromQuery;
      this.scoreMode = scoreMode;
      this.fromField = fromField;
      this.toField = toField;
    }

    @Override
    public Weight createWeight(
        IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
        throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      final Query jq =
          JoinUtil.createJoinQuery(
              fromField, true, toField, fromQuery, info.getReq().getSearcher(), this.scoreMode);
      return jq.rewrite(searcher.getIndexReader()).createWeight(searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
      return "SameCoreJoinQuery [fromQuery="
          + fromQuery
          + ", fromField="
          + fromField
          + ", toField="
          + toField
          + ", scoreMode="
          + scoreMode
          + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = classHash();
      result = prime * result + Objects.hashCode(fromField);
      result = prime * result + Objects.hashCode(fromQuery);
      result = prime * result + Objects.hashCode(scoreMode);
      result = prime * result + Objects.hashCode(toField);
      return result;
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(SameCoreJoinQuery other) {
      return Objects.equals(fromField, other.fromField)
          && Objects.equals(fromQuery, other.fromQuery)
          && Objects.equals(scoreMode, other.scoreMode)
          && Objects.equals(toField, other.toField);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String fromField = localParams.get("from");
        final String fromIndex = localParams.get("fromIndex");
        final String toField = localParams.get("to");
        final ScoreMode scoreMode = ScoreModeParser.parse(getParam(SCORE));

        final String v = localParams.get(CommonParams.VALUE);

        return createQuery(
            fromField,
            v,
            fromIndex,
            toField,
            scoreMode,
            CommonParams.TRUE.equals(localParams.get("TESTenforceSameCoreAsAnotherOne")));
      }

      private Query createQuery(
          final String fromField,
          final String fromQueryStr,
          String fromIndex,
          final String toField,
          final ScoreMode scoreMode,
          boolean byPassShortCircutCheck)
          throws SyntaxError {

        final String myCore = req.getCore().getCoreDescriptor().getName();

        if (fromIndex != null && (!fromIndex.equals(myCore) || byPassShortCircutCheck)) {
          CoreContainer container = req.getCoreContainer();

          final String coreName =
              getCoreName(fromIndex, container, req.getCore(), toField, fromField, localParams);
          final SolrCore fromCore = container.getCore(coreName);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + coreName);
          }

          long fromCoreOpenTime = 0;
          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);

          try {
            QParser fromQueryParser = QParser.getParser(fromQueryStr, otherReq);
            Query fromQuery = fromQueryParser.getQuery();

            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) {
              fromCoreOpenTime = fromHolder.get().getOpenNanoTime();
            }
            return new OtherCoreJoinQuery(
                fromQuery, fromField, coreName, fromCoreOpenTime, scoreMode, toField);
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          QParser fromQueryParser = subQuery(fromQueryStr, null);
          final Query fromQuery = fromQueryParser.getQuery();
          return new SameCoreJoinQuery(fromQuery, fromField, toField, scoreMode);
        }
      }
    };
  }

  /**
   * Returns a String with the name of a core.
   *
   * <p>This method searches the core with fromIndex name in the core's container. If fromIndex
   * isn't the name of a collection or alias it returns fromIndex without changes. If fromIndex is
   * the name of an alias but the alias points to multiple collections it throws
   * SolrException.ErrorCode.BAD_REQUEST because multiple shards not yet supported.
   *
   * @param fromIndex name of the index
   * @param container the core container for searching the core with fromIndex name or alias
   * @param toCore core which it joins to ie executing this request
   * @param toField to side field
   * @param fromField from side field
   * @param localParams local params for this qparser invocation
   * @return the string with name of core
   */
  public static String getCoreName(
      final String fromIndex,
      CoreContainer container,
      SolrCore toCore,
      String toField,
      String fromField,
      SolrParams localParams) {
    if (container.isZooKeeperAware()) {
      ZkController zkController = container.getZkController();
      final String fromCollection = resolveAlias(fromIndex, zkController);
      // TODO DWS: no need for this since later, clusterState.getCollection will throw a reasonable
      // error
      if (!zkController.getClusterState().hasCollection(fromCollection)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "SolrCloud join: Collection '" + fromIndex + "' not found!");
      }
      return findLocalReplicaForFromIndex(
          zkController, fromCollection, toCore, toField, fromField, localParams);
    }
    return fromIndex;
  }

  /**
   * A helper method for other plugins to create single-core JoinQueries
   *
   * @param subQuery the query to define the starting set of documents on the "left side" of the
   *     join
   * @param fromField "left side" field name to use in the join
   * @param toField "right side" field name to use in the join
   * @param scoreMode the score statistic to produce while joining
   * @see JoinQParserPlugin#createJoinQuery(Query, String, String, String)
   */
  public static Query createJoinQuery(
      Query subQuery, String fromField, String toField, ScoreMode scoreMode) {
    return new SameCoreJoinQuery(subQuery, fromField, toField, scoreMode);
  }

  private static String resolveAlias(String fromIndex, ZkController zkController) {
    final Aliases aliases = zkController.getZkStateReader().getAliases();
    try {
      return aliases.resolveSimpleAlias(fromIndex); // if not an alias, returns input
    } catch (IllegalArgumentException e) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "SolrCloud join: Collection alias '"
              + fromIndex
              + "' maps to multiple collectiions, which is not currently supported for joins.",
          e);
    }
  }

  private static String findLocalReplicaForFromIndex(
      ZkController zkController,
      String fromIndex,
      SolrCore toCore,
      String toField,
      String fromField,
      SolrParams localParams) {
    final DocCollection fromCollection = zkController.getClusterState().getCollection(fromIndex);
    final String nodeName = zkController.getNodeName();
    if (fromCollection.getSlices().size() == 1) {
      return getLocalSingleShard(zkController, fromIndex);
    } else { // sharded from
      final CloudDescriptor toCoreDescriptor = toCore.getCoreDescriptor().getCloudDescriptor();
      final DocCollection toCollection =
          zkController.getClusterState().getCollection(toCoreDescriptor.getCollectionName());

      final String shardId = toCoreDescriptor.getShardId();
      boolean isFromSideCheckRequired =
          checkToSideRouter(toCore, toField, localParams, fromCollection, toCollection);

      checkSliceRanges(toCollection, fromCollection, shardId);
      return findCollocatedFromCore(
          toCore, fromField, fromCollection, nodeName, isFromSideCheckRequired);
    }
  }

  private static String findCollocatedFromCore(
      SolrCore toCore,
      String fromField,
      DocCollection fromCollection,
      String nodeName,
      boolean isFromSideCheckRequired) {
    final CloudDescriptor toCoreDescriptor = toCore.getCoreDescriptor().getCloudDescriptor();
    String toShardId = toCoreDescriptor.getShardId();
    final Slice fromShardReplicas = fromCollection.getActiveSlicesMap().get(toShardId);
    for (Replica collocatedFrom :
        fromShardReplicas.getReplicas(r -> r.getNodeName().equals(nodeName))) {
      final String fromCore = collocatedFrom.getCoreName();
      if (log.isDebugEnabled()) {
        log.debug("<-{} @ {}", fromCore, toCoreDescriptor.getCoreNodeName());
      }
      // which replica to pick if there are many one?
      // if router field is not set, "from" may fall back to uniqueKey, but only we attempt to pick
      // local shard.
      if (isFromSideCheckRequired) {
        SolrCore fromCoreOnDemand[] = new SolrCore[1];
        try {
          checkRouterField(
              () ->
                  fromCoreOnDemand[0] == null
                      ? fromCoreOnDemand[0] = toCore.getCoreContainer().getCore(fromCore)
                      : fromCoreOnDemand[0],
              fromCollection,
              fromField);
        } finally {
          if (fromCoreOnDemand[0] != null) {
            fromCoreOnDemand[0].close();
          }
        }
      }
      return fromCore;
    }
    final String shards =
        fromShardReplicas.getReplicas().stream()
            .map(r -> r.getShard() + ":" + r.getCoreName() + "@" + r.getNodeName())
            .collect(Collectors.joining(","));
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Unable to find collocated \"from\" replica: "
            + shards
            + " at node: "
            + nodeName
            + " for "
            + toShardId
            + ". "
            + USE_CROSSCOLLECTION);
  }

  private static boolean checkToSideRouter(
      SolrCore toCore,
      String toField,
      SolrParams localParams,
      DocCollection fromCollection,
      DocCollection toCollection) {
    String routerName = checkRouters(toCollection, fromCollection);
    boolean checkFromRouterField = false;
    switch (routerName) {
      case PlainIdRouter.NAME: // mandatory field check
        checkFromRouterField = true;
        checkRouterField(() -> toCore, toCollection, toField);
        break;
      case CompositeIdRouter.NAME: // let you shoot your legs
        if (localParams.getBool(CHECK_ROUTER_FIELD, true)) {
          checkFromRouterField = true;
          checkRouterField(() -> toCore, toCollection, toField);
        }
        break;
      case ImplicitDocRouter.NAME: // don't check field, you know what you do
      default:
        // if router field is not set, "to" may fallback to uniqueKey
        if (localParams.getBool(CHECK_ROUTER_FIELD, true)) {
          checkRouterField(() -> toCore, toCollection, toField);
        }
        break;
    }
    return checkFromRouterField;
  }

  /**
   * Finds a core of collocated single shard collection.
   *
   * @return core name of the collocated single shard collection
   */
  private static String getLocalSingleShard(ZkController zkController, String fromIndex) {
    final DocCollection fromCollection = zkController.getClusterState().getCollection(fromIndex);
    final String nodeName = zkController.getNodeName();
    String fromReplica = null;

    for (Slice slice : fromCollection.getActiveSlicesArr()) {
      assert fromReplica == null;
      for (Replica replica : slice.getReplicas()) {
        if (replica.getNodeName().equals(nodeName)) {
          fromReplica = replica.getStr(ZkStateReader.CORE_NAME_PROP);
          // found local replica, but is it Active?
          if (replica.getState() != Replica.State.ACTIVE)
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "SolrCloud join: "
                    + fromIndex
                    + " has a local replica ("
                    + fromReplica
                    + ") on "
                    + nodeName
                    + ", but it is "
                    + replica.getState());

          break;
        }
      }
    }

    if (fromReplica == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, USE_CROSSCOLLECTION);

    return fromReplica;
  }

  private static void checkSliceRanges(
      DocCollection toCollection, DocCollection fromCollection, String shardId) {
    final DocRouter.Range toRange = toCollection.getSlice(shardId).getRange();
    final DocRouter.Range fromRange = fromCollection.getSlice(shardId).getRange();
    if (toRange == null && fromRange == null) {
      // perhaps these are implicit routers
      return;
    }
    if ((toRange == null) != (fromRange == null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Expecting non null slice ranges for shard:"
              + shardId
              + " of "
              + toCollection
              + " "
              + toRange
              + " and "
              + fromCollection
              + " "
              + fromRange
              + ". "
              + USE_CROSSCOLLECTION);
    }
    if (!toRange.isSubsetOf(fromRange)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Expecting hash range of collection:"
              + toCollection.getName()
              + " of shard:"
              + shardId
              + " "
              + toRange
              + " to be a subset of the same shard of collection:"
              + fromCollection.getName()
              + ", which is "
              + fromRange
              + ". "
              + USE_CROSSCOLLECTION);
    }
  }

  private static void checkRouterField(
      Supplier<SolrCore> toCore, DocCollection fromCollection, String fromField) {
    String routeField = fromCollection.getRouter().getRouteField(fromCollection);
    if (routeField == null) {
      routeField = toCore.get().getLatestSchema().getUniqueKeyField().getName();
    }
    if (!fromField.equals(routeField)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "collection:"
              + fromCollection.getName()
              + " is sharded by: '"
              + routeField
              + "', but attempting to join via '"
              + fromField
              + "' field. "
              + USE_CROSSCOLLECTION);
    }
  }

  private static String checkRouters(DocCollection collection, DocCollection fromCollection) {
    final String routerName = collection.getRouter().getName();
    if (!routerName.equals(fromCollection.getRouter().getName())) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          collection.getName()
              + " and "
              + fromCollection.getName()
              + " should have the same routers, but they are: "
              + collection.getRouter().getName()
              + " and "
              + fromCollection.getRouter().getName()
              + ". "
              + USE_CROSSCOLLECTION);
    }
    return routerName;
  }
}
