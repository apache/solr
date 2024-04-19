/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.search;

import static org.apache.solr.monitor.search.PresearcherFactory.PresearcherParameters;

import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class ReverseQueryParserPlugin extends QParserPlugin implements ResourceLoaderAware {

  public static final String NAME = "reverse";

  // The default prefix is long because we don't want it to clash
  // with a user-defined name pattern and the longest one wins.
  // {@link
  // https://cwiki.apache.org/confluence/display/solr/SchemaXml#Dynamic_fields:~:text=Longer%20patterns%20will%20be%20matched%20first}
  // In the worst case this can be overridden by the user but ideally this never comes up.
  private static final String DEFAULT_ALIAS_PREFIX =
      "________________________________monitor_alias_";

  private Presearcher presearcher;
  private PresearcherParameters presearcherParameters;

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ReverseQueryParser(qstr, localParams, params, req, presearcher);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    String presearcherType = (String) args.get("presearcherType");
    String termWeightorType = (String) args.get("termWeightorType");
    boolean applyFieldNameAlias =
        resolveProperty(args, "applyFieldNameAlias", Boolean.class, false);
    int numberOfPasses = resolveProperty(args, "numberOfPasses", Integer.class, 0);
    float minWeight = resolveProperty(args, "minWeight", Float.class, 0f);
    String aliasPrefix =
        Optional.ofNullable((String) args.get("aliasPrefix")).orElse(DEFAULT_ALIAS_PREFIX);
    presearcherParameters =
        new PresearcherParameters(
            presearcherType,
            termWeightorType,
            applyFieldNameAlias,
            numberOfPasses,
            minWeight,
            aliasPrefix);
  }

  // TODO is there a better pattern for this? NamedList::get(String key, T default) can't be called
  // on NamedList<?>
  @SuppressWarnings("unchecked")
  private <T> T resolveProperty(
      NamedList<?> args, String propertyName, Class<T> clazz, T defaultVal) {
    Object obj = args.get(propertyName);
    if (obj == null) {
      return defaultVal;
    }
    if (clazz.isInstance(obj)) {
      return (T) obj;
    }
    throw new IllegalArgumentException(propertyName + " must be a " + clazz.getSimpleName());
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    presearcher = PresearcherFactory.build(loader, presearcherParameters);
  }

  public Presearcher getPresearcher() {
    return presearcher;
  }
}
