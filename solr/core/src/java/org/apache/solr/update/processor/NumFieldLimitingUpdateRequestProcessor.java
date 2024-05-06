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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Update request processor created by {@link NumFieldLimitingUpdateRequestProcessorFactory} */
public class NumFieldLimitingUpdateRequestProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrQueryRequest req;
  private int fieldThreshold;
  private int currentNumFields;
  private boolean warnOnly;

  public NumFieldLimitingUpdateRequestProcessor(
      SolrQueryRequest req,
      UpdateRequestProcessor next,
      int fieldThreshold,
      int currentNumFields,
      boolean warnOnly) {
    super(next);
    this.req = req;
    this.fieldThreshold = fieldThreshold;
    this.currentNumFields = currentNumFields;
    this.warnOnly = warnOnly;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (coreExceedsFieldLimit()) {
      throwExceptionOrLog(cmd.getPrintableId());
    } else {
      if (log.isDebugEnabled()) {
        log.debug(
            "Allowing document {}, since current core is under the 'maxFields' limit (numFields={}, maxFields={})",
            cmd.getPrintableId(),
            currentNumFields,
            fieldThreshold);
      }
    }
    super.processAdd(cmd);
  }

  protected boolean coreExceedsFieldLimit() {
    return currentNumFields > fieldThreshold;
  }

  protected void throwExceptionOrLog(String id) {
    final String messageSuffix = warnOnly ? "Blocking update of document " + id : "";
    final String message =
        String.format(
            Locale.ROOT,
            "Current core has %d fields, exceeding the max-fields limit of %d.  %s",
            currentNumFields,
            fieldThreshold,
            messageSuffix);
    if (warnOnly) {
      log.warn(message);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
    }
  }
}
