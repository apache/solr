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
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Factory for {@link ContentHashVersionProcessor} instances.
 *
 * <p>This processor computes a hash from the document content and sees if an existing (indexed)
 * document also has this hash, stored in a designated field. If so, it drops the document to avoid
 * needless index churn. Alternatively, it can be configured to merely log the fact.
 *
 * <h2>Configuration</h2>
 *
 * <ul>
 *   <li><b>hashField</b> (required): The name of the field where the computed hash will be stored.
 *       It should be of type BinaryField. This field must have docValues enabled for hash
 *       retrieval. The hash field is automatically excluded from hash computation.
 *   <li><b>includeFields</b> (optional, default="*"): Comma-separated list of fields to include in
 *       hash computation. Supports wildcard patterns (e.g., "name*"). Use "*" to include all
 *       fields.
 *   <li><b>excludeFields</b> (optional): Comma-separated list of fields to exclude from hash
 *       computation. Supports wildcard patterns. Cannot be "*" (cannot exclude all fields).
 *   <li><b>hashCompareStrategy</b> (optional, default="drop"): Controls behavior when duplicate
 *       content is detected:
 *       <ul>
 *         <li>"drop": Silently drops documents with matching hash (no-op updates)
 *         <li>"log": Logs duplicate detection but still processes the update
 *       </ul>
 * </ul>
 *
 * <h2>Configuration Example</h2>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ContentHashVersionProcessorFactory"&gt;
 *   &lt;str name="hashField"&gt;content_hash&lt;/str&gt;
 *   &lt;str name="includeFields"&gt;title,body,author&lt;/str&gt;
 *   &lt;str name="excludeFields"&gt;timestamp,version&lt;/str&gt;
 *   &lt;str name="hashCompareStrategy"&gt;drop&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <h2>Important Considerations</h2>
 *
 * <ul>
 *   <li><b>In-Place Updates</b>: Fields updated via in-place (partial) updates should be excluded
 *       from hash computation using <code>excludeFields</code>, as these are updated independently
 *       and should not affect duplicate detection.
 * </ul>
 *
 * <h2>Monitoring</h2>
 *
 * <p>The processor logs duplicate statistics in the response:
 *
 * <ul>
 *   <li><code>contentHash.duplicatesDropped</code>: Count of duplicates dropped (when
 *       hashCompareStrategy=drop)
 *   <li><code>contentHash.duplicatesDetected</code>: Count of duplicates detected (when
 *       hashCompareStrategy=log)
 * </ul>
 *
 * @see ContentHashVersionProcessor
 * @see Lookup3Signature
 */
public class ContentHashVersionProcessorFactory extends UpdateRequestProcessorFactory {
  private static final char SEPARATOR = ','; // Separator for included/excluded fields
  private List<String> includeFields = List.of("*"); // Included fields defaults to 'all'
  private List<String> excludeFields = new ArrayList<>();
  private String hashField; // Must be explicitly configured
  private boolean dropSameDocuments = true;

  public ContentHashVersionProcessorFactory() {}

  @Override
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
    tmp = args.remove("hashField");
    if (tmp == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "'hashField' is required and must be explicitly configured");
    }
    if (!(tmp instanceof String)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "'hashField' must be configured as a <str>");
    }
    this.hashField = (String) tmp;

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
    excludeFields.add(hashField); // Hash field name is excluded from hash computation

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

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new ContentHashVersionProcessor(
        buildFieldMatcher(includeFields),
        buildFieldMatcher(excludeFields),
        hashField,
        dropSameDocuments,
        req,
        rsp,
        next);
  }

  public String getHashField() {
    return hashField;
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

  static Predicate<SolrInputField> buildFieldMatcher(List<String> fieldNames) {
    return inputField -> {
      for (String currentFieldName : fieldNames) {
        if ("*".equals(currentFieldName)) {
          return true;
        }
        final String fieldName = inputField.getName();
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
