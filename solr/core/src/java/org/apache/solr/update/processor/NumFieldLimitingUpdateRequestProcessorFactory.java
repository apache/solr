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
package org.apache.solr.update.processor;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

public class NumFieldLimitingUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware {

  private static final String MAXIMUM_FIELDS_PARAM = "maxFields";
  private static final String WARN_ONLY_PARAM = "warnOnly";

  private NumFieldsMonitor numFieldsMonitor;
  private int maximumFields;
  private boolean warnOnly;

  @Override
  public void inform(final SolrCore core) {
    // register a commit callback for monitoring the number of fields in the schema
    numFieldsMonitor = new NumFieldsMonitor(core);
    core.getUpdateHandler().registerCommitCallback(numFieldsMonitor);
    core.registerNewSearcherListener(numFieldsMonitor);
  }

  public void init(NamedList<?> args) {
    warnOnly = args.indexOf(WARN_ONLY_PARAM, 0) > 0 ? args.getBooleanArg(WARN_ONLY_PARAM) : false;

    if (args.indexOf(MAXIMUM_FIELDS_PARAM, 0) < 0) {
      throw new IllegalArgumentException(
          "The "
              + MAXIMUM_FIELDS_PARAM
              + " parameter is required for "
              + getClass().getName()
              + ", but no value was provided.");
    }
    final Object rawMaxFields = args.get(MAXIMUM_FIELDS_PARAM);
    if (rawMaxFields == null || !(rawMaxFields instanceof Integer)) {
      throw new IllegalArgumentException(
          MAXIMUM_FIELDS_PARAM + " must be configured as a non-null <int>");
    }
    maximumFields = (Integer) rawMaxFields;
    if (maximumFields <= 0) {
      throw new IllegalArgumentException(MAXIMUM_FIELDS_PARAM + " must be a positive integer");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new NumFieldLimitingUpdateRequestProcessor(
        req, next, maximumFields, numFieldsMonitor.getCurrentNumFields(), warnOnly);
  }

  public int getFieldThreshold() {
    return maximumFields;
  }

  public boolean getWarnOnly() {
    return warnOnly;
  }
}
