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
package org.apache.solr.ltr;

/**
 * FeatureLogger can be registered in a model and provide a strategy for logging the feature values.
 */
public abstract class FeatureLogger {
  public enum FeatureFormat {
    DENSE,
    SPARSE
  };

  protected final FeatureFormat featureFormat;

  protected Boolean logAll;

  protected boolean logFeatures;

  protected FeatureLogger(FeatureFormat f, Boolean logAll) {
    this.featureFormat = f;
    this.logAll = logAll;
    this.logFeatures = false;
  }

  public abstract String printFeatureVector(LTRScoringQuery.FeatureInfo[] featuresInfo);

  public Boolean isLoggingAll() {
    return logAll;
  }

  public void setLogAll(Boolean logAll) {
    this.logAll = logAll;
  }

  public void setLogFeatures(boolean logFeatures) {
    this.logFeatures = logFeatures;
  }

  public boolean isLogFeatures() {
    return logFeatures;
  }
}
