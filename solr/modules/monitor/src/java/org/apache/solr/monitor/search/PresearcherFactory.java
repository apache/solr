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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import org.apache.lucene.monitor.CustomQueryHandler;
import org.apache.lucene.monitor.MultipassTermFilteredPresearcher;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.TermFilteredPresearcher;
import org.apache.lucene.monitor.TermWeightor;
import org.apache.lucene.util.ResourceLoader;
import org.apache.solr.monitor.AliasingMultipassTermFilteredPresearcher;
import org.apache.solr.monitor.AliasingTermFilteredPresearcher;

public class PresearcherFactory {

  // The default prefix is long because we don't want it to clash
  // with a user-defined name pattern and the longest one wins.
  // {@link
  // https://cwiki.apache.org/confluence/display/solr/SchemaXml#Dynamic_fields:~:text=Longer%20patterns%20will%20be%20matched%20first}
  // In the worst case this can be overridden by the user but ideally this never comes up.
  public static final String DEFAULT_ALIAS_PREFIX =
      "_________________________________________________monitor_alias_";

  public static final String TERM_FILTERED = TermFilteredPresearcher.class.getSimpleName();
  public static final String MULTI_PASS_TERM_FILTERED =
      MultipassTermFilteredPresearcher.class.getSimpleName();

  private PresearcherFactory() {}

  static Presearcher build(
      ResourceLoader resourceLoader, PresearcherParameters presearcherParameters) {
    Presearcher presearcher;
    String type = presearcherParameters.presearcherType;
    if (TERM_FILTERED.equals(type) || MULTI_PASS_TERM_FILTERED.equals(type)) {
      TermWeightor weightor =
          constructZeroArgClass(
              resourceLoader,
              TermWeightor.class,
              presearcherParameters.termWeightorType,
              TermFilteredPresearcher.DEFAULT_WEIGHTOR);
      List<CustomQueryHandler> customQueryHandlers = List.of();
      Set<String> filterFields = Set.of();
      if (TERM_FILTERED.equals(type)) {
        if (presearcherParameters.applyFieldNameAlias) {
          presearcher =
              AliasingTermFilteredPresearcher.build(
                  weightor, customQueryHandlers, filterFields, presearcherParameters.aliasPrefix);
        } else {
          presearcher = new TermFilteredPresearcher(weightor, customQueryHandlers, filterFields);
        }
      } else {
        if (presearcherParameters.applyFieldNameAlias) {
          presearcher =
              AliasingMultipassTermFilteredPresearcher.build(
                  presearcherParameters.numberOfPasses,
                  presearcherParameters.minWeight,
                  weightor,
                  customQueryHandlers,
                  filterFields,
                  presearcherParameters.aliasPrefix);
        } else {
          presearcher =
              new MultipassTermFilteredPresearcher(
                  presearcherParameters.numberOfPasses,
                  presearcherParameters.minWeight,
                  weightor,
                  customQueryHandlers,
                  filterFields);
        }
      }
    } else {
      presearcher =
          constructZeroArgClass(resourceLoader, Presearcher.class, type, Presearcher.NO_FILTERING);
    }
    return presearcher;
  }

  private static <T> T constructZeroArgClass(
      ResourceLoader loader, Class<T> expectedType, String typeName, T defaultVal) {
    if (typeName == null) {
      return defaultVal;
    }
    try {
      Class<? extends T> implClass = loader.findClass(typeName, expectedType);
      Constructor<? extends T> c = implClass.getConstructor();
      return c.newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Could not construct custom " + expectedType.getSimpleName() + " of type " + typeName, e);
    }
  }

  public static class PresearcherParameters {

    private String presearcherType;
    private String termWeightorType;
    private boolean applyFieldNameAlias = false;
    private int numberOfPasses = 0;
    private float minWeight = 0;
    private String aliasPrefix = DEFAULT_ALIAS_PREFIX;

    public void setPresearcherType(String presearcherType) {
      this.presearcherType = presearcherType;
    }

    public void setTermWeightorType(String termWeightorType) {
      this.termWeightorType = termWeightorType;
    }

    public void setApplyFieldNameAlias(boolean applyFieldNameAlias) {
      this.applyFieldNameAlias = applyFieldNameAlias;
    }

    public void setNumberOfPasses(int numberOfPasses) {
      this.numberOfPasses = numberOfPasses;
    }

    public void setMinWeight(float minWeight) {
      this.minWeight = minWeight;
    }

    public void setAliasPrefix(String aliasPrefix) {
      this.aliasPrefix = aliasPrefix;
    }
  }
}
