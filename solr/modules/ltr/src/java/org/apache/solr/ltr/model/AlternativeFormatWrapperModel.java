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

package org.apache.solr.ltr.model;

import java.util.List;
import java.util.Map;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;

/**
 * ... when to use this class ... ... currently only JSON format is supported. Contributions to
 * support further formats are welcome.
 *
 * <p>Example configuration:
 *
 * <pre>
 * {
 *   "class": "org.apache.solr.ltr.model.AlternativeFormatWrapperModel",
 *   "name": "myJsonWrapperModelName",
 *   "params": {
 *     "format": "json",
 *     "content": "{ \"class\": \"org.apache.solr.ltr.model.LinearModel\", \"name\": \"myModelName\", \"params\": { ... } }"
 *   }
 * }
 * </pre>
 */
public class AlternativeFormatWrapperModel extends WrapperModel { // TODO: find a better name?

  /**
   * format is part of the LTRScoringModel params map and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private String format;

  /**
   * content is part of the LTRScoringModel params map and therefore here it does not individually
   * influence the class hashCode, equals, etc.
   */
  private String content;

  public AlternativeFormatWrapperModel(
      String name,
      List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName,
      List<Feature> allFeatures,
      Map<String, Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public void setContent(String content) {
    this.content = content;
  }

  @Override
  protected void validate() throws ModelException {
    super.validate();
    if (format == null) {
      throw new ModelException("no format configured for model " + name);
    }
    if (content == null) {
      throw new ModelException("no content configured for model " + name);
    }
  }

  @Override
  public Map<String, Object> fetchModelMap() throws ModelException {
    Map<String, Object> modelMapObj = null;
    // parse 'content' into object according to 'format'
    // throw ModelException if format is unsupported or content parsing fails
    return modelMapObj;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("(name=").append(getName());
    sb.append(",format=").append(format);
    sb.append(",content=").append(content);
    sb.append(",model=(").append(model.toString()).append(")");

    return sb.toString();
  }
}
