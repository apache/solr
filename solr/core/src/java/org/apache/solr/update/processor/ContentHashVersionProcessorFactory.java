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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/** Factory for {@link ContentHashVersionProcessor} instances. */
public class ContentHashVersionProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final char SEPARATOR = ','; // Separator for included/excluded fields
  private List<String> includeFields = List.of("*"); // Included fields defaults to 'all'
  private List<String> excludeFields = new ArrayList<>();
  private String hashFieldName; // Must be explicitly configured
  private boolean dropSameDocuments = true;

  public ContentHashVersionProcessorFactory() {}

  public void init(NamedList<?> args) {
    Object tmp = args.remove("includeFields");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "'includeFields' must be configured as a <str>");
      }
      // Include fields support comma separated list of fields (e.g. "field1,field2,field3").
      // Also supports "*" to include all fields
      this.includeFields =
          StrUtils.splitSmart((String) tmp, SEPARATOR).stream()
              .map(String::trim)
              .collect(Collectors.toList());
    }
    tmp = args.remove("hashFieldName");
    if (tmp == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "'hashFieldName' is required and must be explicitly configured");
    }
    if (!(tmp instanceof String)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "'hashFieldName' must be configured as a <str>");
    }
    this.hashFieldName = (String) tmp;

    tmp = args.remove("excludeFields");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "'excludeFields' must be configured as a <str>");
      }
      if ("*".equals(((String) tmp).trim())) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "'excludeFields' can't exclude all fields.");
      }
      // Exclude fields support comma separated list of fields (e.g.
      // "excluded_field1,excluded_field2").
      // Also supports "*" to exclude all fields
      this.excludeFields =
          StrUtils.splitSmart((String) tmp, SEPARATOR).stream()
              .map(String::trim)
              .collect(Collectors.toList());
    }
    excludeFields.add(hashFieldName); // Hash field name is excluded from hash computation

    tmp = args.remove("hashCompareStrategy");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "'hashCompareStrategy' must be configured as a <str>");
      }
      String value = ((String) tmp).toLowerCase(Locale.ROOT);
      if ("drop".equalsIgnoreCase(value)) {
        dropSameDocuments = true;
      } else if ("log".equalsIgnoreCase(value)) {
        dropSameDocuments = false;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Value '"
                + value
                + "' is unsupported for 'hashCompareStrategy', only 'drop' and 'log' are supported.");
      }
    }

    super.init(args);
  }

  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    ContentHashVersionProcessor processor =
        new ContentHashVersionProcessor(
            buildFieldMatcher(includeFields),
            buildFieldMatcher(excludeFields),
            hashFieldName,
            req,
            rsp,
            next);
    processor.setDropSameDocuments(dropSameDocuments);
    return processor;
  }

  public void inform(SolrCore core) {
    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "schema must have uniqueKey defined.");
    }
  }

  public String getHashFieldName() {
    return hashFieldName;
  }

  public List<String> getIncludeFields() {
    return includeFields;
  }

  public List<String> getExcludeFields() {
    return excludeFields;
  }

  public boolean dropSameDocuments() {
    return dropSameDocuments;
  }

  static Predicate<String> buildFieldMatcher(List<String> fieldNames) {
    return fieldName -> {
      for (String currentFieldName : fieldNames) {
        if ("*".equals(currentFieldName)) {
          return true;
        }
        if (fieldName.equals(currentFieldName)) {
          return true;
        }
        if (currentFieldName.length() > 1
            && currentFieldName.endsWith("*")
            && fieldName.startsWith(currentFieldName.substring(0, currentFieldName.length() - 1))) {
          return true;
        }
      }
      return false;
    };
  }
}
