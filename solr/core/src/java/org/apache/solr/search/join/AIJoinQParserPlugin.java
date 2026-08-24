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
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.join.aijoin.AIJoinIndex;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query parser exercising {@link AIJoinIndex} inside a {@link SolrCore}: it mimics {@link
 * ScoreJoinQParserPlugin}'s local parameters, but resolves matches through the sidecar join index
 * instead of {@link org.apache.lucene.search.join.JoinUtil}. Local parameters:
 *
 * <ul>
 *   <li>from - "foreign key" field name, collected while enumerating the subordinate query (the
 *       local parameter value).
 *   <li>fromIndex - optional core name to run the subordinate query against, when it differs from
 *       this core; cross-core joins are the reason {@link AIJoinIndex} exists in the first place,
 *       so this mirrors {@link ScoreJoinQParserPlugin}'s <code>fromIndex</code>, including
 *       SolrCloud alias/collection resolution via {@link ScoreJoinQParserPlugin#getCoreName}.
 *   <li>to - "primary key" field name looked up in this core's index.
 * </ul>
 *
 * Example: {@code q={!aijoin from=manu_id_s to=id fromIndex=products}foo}.
 *
 * <p>Unlike {@link ScoreJoinQParserPlugin.OtherCoreJoinQuery}, which only borrows the from-side
 * searcher long enough to build a self-contained {@code Query} in {@code createWeight}, an {@link
 * org.apache.solr.search.join.aijoin.AIJoinQuery} keeps reading the from-side searcher on every
 * {@code scorerSupplier} call (it may lazily build missing pair columns per to-segment), so a
 * cross-core from-searcher is pinned open for the whole request via {@link
 * SolrRequestInfo#addCloseHook}, the same mechanism {@link
 * org.apache.solr.search.JoinQuery.JoinQueryWeight} uses for the regular {@code {!join}}.
 *
 * <p>One {@link AIJoinIndex} is opened per core in {@link #inform(SolrCore)}, backed by a directory
 * under the core's dataDir (configurable via the {@code dir} init parameter, resolved relative to
 * dataDir unless absolute), and closed when the core closes. This sidecar always belongs to the
 * "to" side core -- the one this plugin is registered in.
 *
 * <p><b>Why this implements {@link QueryResponseWriter}:</b> {@link
 * org.apache.solr.core.SolrResourceLoader}'s {@code awareCompatibility} allowlist (see SOLR-8311)
 * only lets specific plugin base types implement {@link SolrCoreAware}, and {@code QParserPlugin}
 * isn't one of them, so a plain {@code implements SolrCoreAware} fails core load with "Invalid
 * 'Aware' object". {@code QueryResponseWriter} is on the allowlist and happens to be the cheapest
 * interface there to satisfy (two abstract methods, both unreachable stubs below -- this class is
 * never registered as a {@code <queryResponseWriter>}). This is safe here specifically because
 * {@code QParserPlugin} instances are loaded once per core load/reload via {@link
 * org.apache.solr.core.PluginBag}, exactly like the already-whitelisted {@link
 * org.apache.solr.handler.component.SearchComponent} -- never created ad-hoc per request ({@link
 * QParser#getParser(String, SolrQueryRequest)} resolves the already registered instance via {@code
 * req.getCore().getQueryPlugin(name)}).
 */
public class AIJoinQParserPlugin extends QParserPlugin
    implements QueryResponseWriter, SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Init parameter: directory holding the sidecar join index, resolved against the core's dataDir
   * unless absolute. Defaults to {@value #DEFAULT_DIR}.
   */
  public static final String DIR = "dir";

  public static final String NAME = "auxIndexJoin";

  public static final String DEFAULT_DIR = "aux-index-join";

  private String configuredDir = DEFAULT_DIR;

  private volatile AIJoinIndex joinIndex;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    if (args != null && args.get(DIR) != null) {
      configuredDir = args.get(DIR).toString();
    }
  }

  @Override
  public void inform(SolrCore core) {
    Path path = Path.of(configuredDir);
    if (!path.isAbsolute()) {
      path = Path.of(core.getDataDir()).resolve(path);
    } else {
      core.getCoreContainer().assertPathAllowed(path);
    }
    Directory directory = null;
    try {
      directory =
          core.getDirectoryFactory()
              .get(path.toString(), DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      joinIndex = new AIJoinIndex(directory);
    } catch (IOException | RuntimeException e) {
      if (directory != null) {
        try {
          core.getDirectoryFactory().release(directory);
        } catch (IOException releaseException) {
          e.addSuppressed(releaseException);
        }
      }
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed to open AIJoinIndex at " + path, e);
    }
    final Directory capturedDirectory = directory;
    core.addCloseHook(
        new CloseHook() {
          @Override
          public void preClose(SolrCore core) {
            try {
              joinIndex.close();
            } catch (IOException e) {
              log.warn("Failed closing AIJoinIndex", e);
            } finally {
              try {
                core.getDirectoryFactory().release(capturedDirectory);
              } catch (IOException e) {
                log.warn("Failed releasing AIJoinIndex directory {}", capturedDirectory, e);
              }
            }
          }
        });
  }

  // QueryResponseWriter stubs, unreachable: implemented only to satisfy SolrCoreAware's allowlist,
  // see the class javadoc. This plugin is never registered as a <queryResponseWriter>.

  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType) {
    throw new UnsupportedOperationException(
        AIJoinQParserPlugin.class.getSimpleName()
            + " is a QParserPlugin, not a QueryResponseWriter");
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    throw new UnsupportedOperationException(
        AIJoinQParserPlugin.class.getSimpleName()
            + " is a QParserPlugin, not a QueryResponseWriter");
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        if (joinIndex == null) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "AIJoinQParserPlugin is not initialized; is it registered as a <queryParser>?");
        }
        final String fromField = getParam("from");
        final String toField = getParam("to");
        if (fromField == null || toField == null) {
          throw new SyntaxError("aijoin query parser requires 'from' and 'to' local params");
        }
        final String fromIndex = localParams.get("fromIndex");
        final String v = localParams.get(CommonParams.VALUE);
        final String myCore = req.getCore().getCoreDescriptor().getName();

        final Query fromQuery;
        final IndexSearcher fromSearcher;
        ExecutorService fromExecutor;
        if (fromIndex != null && !fromIndex.equals(myCore)) {
          CoreContainer container = req.getCoreContainer();
          String coreName =
              ScoreJoinQParserPlugin.getCoreName(
                  fromIndex, container, req.getCore(), toField, fromField, localParams);
          SolrCore fromCore = container.getCore(coreName);
          if (fromCore == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + coreName);
          }
          SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
          if (info == null) {
            fromCore.close();
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Cross-core aijoin must have SolrRequestInfo");
          }
          // released once this request completes: the from-side searcher is read on every
          // scorerSupplier() call, not just while building this query, so it must outlive parse()
          info.addCloseHook(fromCore);
          try (SolrQueryRequestBase otherReq = new SolrQueryRequestBase(fromCore, params)) {
            fromQuery = QParser.getParser(v, otherReq).getQuery();
          }
          RefCounted<SolrIndexSearcher> fromRef = fromCore.getSearcher(false, true, null);
          info.addCloseHook(fromRef::decref);
          fromSearcher = fromRef.get();
          fromExecutor = (ExecutorService) fromCore.getCoreContainer().getIndexSearcherExecutor();
        } else {
          fromQuery = subQuery(v, null).getQuery();
          fromSearcher = req.getSearcher();
          fromExecutor = (ExecutorService) req.getCoreContainer().getIndexSearcherExecutor();
        }

        return joinIndex.newJoinQuery(fromField, fromQuery, fromSearcher, toField, fromExecutor);
      }
    };
  }
}
