package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Factory for {@link ContentHashVersionProcessor} instances.
 */
public class ContentHashVersionProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final char SEPARATOR = ','; // Separator for included/excluded fields
  static final String CONTENT_HASH_ENABLED_PARAM = "contentHashEnabled";
  private List<String> includeFields = Collections.singletonList("*"); // Included fields defaults to 'all'
  private List<String> excludeFields = new ArrayList<>(); // No excluded field by default, yet hashFieldName is excluded by default
  private String hashFieldName = "content_hash"; // Field name to store computed hash on create/update operations
  private boolean discardSameDocuments = true;

  public ContentHashVersionProcessorFactory() {
  }

  public void init(NamedList<?> args) {
    Object tmp = args.remove("includeFields");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "'includeFields' must be configured as a <str>");
      }
      // Include fields support comma separated list of fields (e.g. "field1,field2,field3").
      // Also supports "*" to include all fields
      this.includeFields = StrUtils.splitSmart((String) tmp, SEPARATOR)
              .stream()
              .map(String::trim)
              .collect(Collectors.toList());
    }
    tmp = args.remove("hashFieldName");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'hashFieldName' must be configured as a <str>");
      }
      this.hashFieldName = (String) tmp;
    }

    tmp = args.remove("excludeFields");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'excludeFields' must be configured as a <str>");
      }
      if ("*".equals(((String) tmp).trim())) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'excludeFields' can't exclude all fields.");
      }
      // Exclude fields support comma separated list of fields (e.g. "excluded_field1,excluded_field2").
      // Also supports "*" to exclude all fields
      this.excludeFields = StrUtils.splitSmart((String) tmp, SEPARATOR)
              .stream()
              .map(String::trim)
              .collect(Collectors.toList());
    }
    excludeFields.add(hashFieldName); // Hash field name is excluded from hash computation

    tmp = args.remove("hashCompareStrategy");
    if (tmp != null) {
      if (!(tmp instanceof String)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'hashCompareStrategy' must be configured as a <str>");
      }
      String value = ((String) tmp).toLowerCase();
      if ("discard".equalsIgnoreCase(value)) {
        discardSameDocuments = true;
      } else if ("log".equalsIgnoreCase(value)) {
        discardSameDocuments = false;
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Value '" + value + "' is unsupported for 'hashCompareStrategy', only 'discard' and 'log' are supported.");
      }
    }

    super.init(args);
  }

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    if (!req.getParams().getBool(CONTENT_HASH_ENABLED_PARAM, false)) {
      return next;
    }

    ContentHashVersionProcessor processor = new ContentHashVersionProcessor(
            buildFieldMatcher(includeFields),
            buildFieldMatcher(excludeFields),
            hashFieldName,
            req,
            rsp,
            next);
    processor.setDiscardSameDocuments(discardSameDocuments);
    return processor;
  }

  public void inform(SolrCore core) {
    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "schema must have uniqueKey defined.");
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

  public boolean discardSameDocuments() {
      return discardSameDocuments;
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
