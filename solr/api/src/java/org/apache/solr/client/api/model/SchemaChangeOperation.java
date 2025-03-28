package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = SchemaChangeOperation.OPERATION_TYPE_PROP)
@JsonSubTypes({
  @JsonSubTypes.Type(value = SchemaChangeOperation.AddFieldType.class, name = "add-field-type"),
  @JsonSubTypes.Type(value = SchemaChangeOperation.AddCopyField.class, name = "add-copy-field"),
  @JsonSubTypes.Type(value = SchemaChangeOperation.AddField.class, name = "add-field"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.AddDynamicField.class,
      name = "add-dynamic-field"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.DeleteFieldType.class,
      name = "delete-field-type"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.DeleteCopyField.class,
      name = "delete-copy-field"),
  @JsonSubTypes.Type(value = SchemaChangeOperation.DeleteField.class, name = "delete-field"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.DeleteDynamicField.class,
      name = "delete-dynamic-field"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.ReplaceFieldType.class,
      name = "replace-field-type"),
  @JsonSubTypes.Type(value = SchemaChangeOperation.ReplaceField.class, name = "replace-field"),
  @JsonSubTypes.Type(
      value = SchemaChangeOperation.ReplaceDynamicField.class,
      name = "replace-dynamic-field"),
})
public class SchemaChangeOperation {

  public static final String OPERATION_TYPE_PROP = "operationType";

  @JsonProperty(OPERATION_TYPE_PROP)
  public String operationType;

  public static class AddFieldType extends SchemaChangeOperation {
    @JsonProperty public String name;

    @JsonProperty("class")
    public String className;

    // Used for setting analyzers, index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  public static class AddCopyField extends SchemaChangeOperation {
    @JsonProperty public String source;
    @JsonProperty public List<String> destinations;
    @JsonProperty public Integer maxChars;
  }

  public static class AddField extends SchemaChangeOperation {
    @JsonProperty public String name;
    @JsonProperty public String type;

    // Used for setting index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  public static class AddDynamicField extends SchemaChangeOperation {
    @JsonProperty public String name;
    @JsonProperty public String type;

    // Used for setting index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  public static class DeleteFieldType extends SchemaChangeOperation {
    @JsonProperty public String name;
  }

  public static class DeleteCopyField extends SchemaChangeOperation {
    @JsonProperty public String source;
    @JsonProperty public List<String> destinations; // TODO Why is this needed on delete?
  }

  public static class DeleteField extends SchemaChangeOperation {
    @JsonProperty public String name;
  }

  public static class DeleteDynamicField extends SchemaChangeOperation {
    @JsonProperty public String name;
  }

  // TODO - I don't love the "replace" name here, that comes from OpTypes, originally
  // Maybe I can change that in the process here to "update" or something similar
  // Maybe that's an additional binding in the JsonSubTypes annotation value above?
  public static class ReplaceFieldType extends SchemaChangeOperation {
    @JsonProperty public String name;

    @JsonProperty("class")
    public String className;

    // Used for setting analyzer, index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  public static class ReplaceField extends SchemaChangeOperation {
    @JsonProperty public String name;
    @JsonProperty public String type;

    // Used for setting index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  public static class ReplaceDynamicField extends SchemaChangeOperation {
    @JsonProperty public String name;
    @JsonProperty public String type;

    // Used for setting index and stored settings, etc.
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }
}
