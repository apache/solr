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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This factory generates an UpdateRequestProcessor which fails update requests once a core has
 * exceeded a configurable maximum number of fields. Meant as a safeguard to help users notice
 * potentially-dangerous schema design before performance and stability problems start to occur.
 *
 * <p>The URP uses the core's {@link SolrIndexSearcher} to judge the current number of fields.
 * Accordingly, it undercounts the number of fields in the core - missing all fields added since the
 * previous searcher was opened. As such, the URP's request-blocking is "best effort" - it cannot be
 * relied on as a precise limit on the number of fields.
 *
 * <p>Additionally, the field-counting includes all documents present in the index, including any
 * deleted docs that haven't yet been purged via segment merging. Note that this can differ
 * significantly from the number of fields defined in managed-schema.xml - especially when dynamic
 * fields are enabled. The only way to reduce this field count is to delete documents and wait until
 * the deleted documents have been removed by segment merges. Users may of course speed up this
 * process by tweaking Solr's segment-merging, triggering an "optimize" operation, etc.
 *
 * <p>{@link NumFieldLimitingUpdateRequestProcessorFactory} accepts two configuration parameters:
 *
 * <ul>
 *   <li><code>maxFields</code> - (required) The maximum number of fields before update requests
 *       should be aborted. Once this limit has been exceeded, additional update requests will fail
 *       until fields have been removed or the "maxFields" is increased.
 *   <li><code>warnOnly</code> - (optional) If <code>true</code> then the URP logs verbose warnings
 *       about the limit being exceeded but doesn't abort update requests. Defaults to <code>false
 *       </code> if not specified
 * </ul>
 *
 * @since 9.7.0
 */
public class NumFieldLimitingUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String MAXIMUM_FIELDS_PARAM = "maxFields";
  private static final String WARN_ONLY_PARAM = "warnOnly";

  // package visibility for tests
  int maximumFields;
  boolean warnOnly;

  @Override
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
    if (!(rawMaxFields instanceof Integer)) {
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
    // note: it's unusual to call req.getSearcher in a /update request but it should be fine
    final int currentNumFields = req.getSearcher().getFieldInfos().size();
    if (currentNumFields <= maximumFields) {
      // great; no need to insert an URP to block or log anything
      return next;
    }

    // Block indexing new documents
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        String id = cmd.getPrintableId();
        final String messageSuffix = warnOnly ? "Blocking update of document " + id : "";
        final String message =
            String.format(
                Locale.ROOT,
                "Current core has %d fields, exceeding the max-fields limit of %d.  %s",
                currentNumFields,
                maximumFields,
                messageSuffix);
        if (warnOnly) {
          log.warn(message);
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
        }
      }
    };
  }
}
