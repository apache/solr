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

  static class PresearcherParameters {

    private final String presearcherType;
    private final String termWeightorType;
    private final boolean applyFieldNameAlias;
    private final int numberOfPasses;
    private final float minWeight;
    private final String aliasPrefix;

    public PresearcherParameters(
        String presearcherType,
        String termWeightorType,
        boolean applyFieldNameAlias,
        int numberOfPasses,
        float minWeight,
        String aliasPrefix) {
      this.presearcherType = presearcherType;
      this.termWeightorType = termWeightorType;
      this.applyFieldNameAlias = applyFieldNameAlias;
      this.numberOfPasses = numberOfPasses;
      this.minWeight = minWeight;
      this.aliasPrefix = aliasPrefix;
    }
  }
}
