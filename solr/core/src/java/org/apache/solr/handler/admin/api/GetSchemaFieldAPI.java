package org.apache.solr.handler.admin.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrClassLoader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.pkg.PackageListeningClassLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;

public class GetSchemaFieldAPI extends GetSchemaAPI{

  private final SolrParams params;

  @Inject
  public GetSchemaFieldAPI(IndexSchema indexSchema, SolrParams params) {
    super(indexSchema);
    this.params = params;
  }

  @GET
  @Path("/fields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListFieldsResponse listSchemaFields() {
    SchemaListFieldsResponse response = instantiateJerseyResponse(SchemaListFieldsResponse.class);
    final String realName = "fields";

    response.fields = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("fields")
    public Object fields;
  }

  @GET
  @Path("/fields/{fieldName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetFieldInfoResponse getFieldInfo(@PathParam("fieldName") String fieldName) {
    SchemaGetFieldInfoResponse response = instantiateJerseyResponse(SchemaGetFieldInfoResponse.class);
    final String realName = "fields";

    SimpleOrderedMap<Object> fieldInfo = retrieveFieldInfoOfType(realName, fieldName, params);
    if (fieldInfo != null) {
      response.fieldInfo = fieldInfo;
      return response;
    }
    throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path /" + realName + "/" + fieldName);
  }

  public static class SchemaGetFieldInfoResponse extends SolrJerseyResponse {
    @JsonProperty("field")
    public SimpleOrderedMap<?> fieldInfo;
  }

  @GET
  @Path("/copyfields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListCopyFieldsResponse listCopyFields() {
    SchemaListCopyFieldsResponse response = instantiateJerseyResponse(SchemaListCopyFieldsResponse.class);
    final String realName = "copyfields";

    response.copyFields = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListCopyFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("copyFields")
    public Object copyFields;
  }

  @GET
  @Path("/dynamicfields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListDynamicFieldsResponse listDynamicFields() {
    SchemaListDynamicFieldsResponse response = instantiateJerseyResponse(SchemaListDynamicFieldsResponse.class);
    final String realName = "dynamicfields";

    response.dynamicFields = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListDynamicFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("dynamicFields")
    public Object dynamicFields;
  }

  @GET
  @Path("/dynamicfields/{fieldName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetDynamicFieldInfoResponse getDynamicFieldInfo(@PathParam("fieldName") String fieldName) {
    SchemaGetDynamicFieldInfoResponse response = instantiateJerseyResponse(SchemaGetDynamicFieldInfoResponse.class);
    final String realName = "dynamicfields";

    SimpleOrderedMap<Object> dynamicFieldInfo = retrieveFieldInfoOfType(realName, fieldName, params);
    if (dynamicFieldInfo != null) {
      response.dynamicFieldInfo = dynamicFieldInfo;
      return response;
    }
    throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path /" + realName + "/" + fieldName);
  }

  public static class SchemaGetDynamicFieldInfoResponse extends SolrJerseyResponse {
    @JsonProperty("dynamicField")
    public SimpleOrderedMap<?> dynamicFieldInfo;
  }

  @GET
  @Path("/fieldtypes")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListFieldTypesResponse listSchemaFieldTypes() {
    SchemaListFieldTypesResponse response = instantiateJerseyResponse(SchemaListFieldTypesResponse.class);
    final String realName = "fieldtypes";

    response.fieldTypes = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListFieldTypesResponse extends SolrJerseyResponse {
    @JsonProperty("fieldTypes")
    public Object fieldTypes;
  }

  @GET
  @Path("/fieldtypes/{fieldTypeName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetFieldTypeInfoResponse getFieldTypeInfo(@PathParam("fieldTypeName") String fieldTypeName) {
    SchemaGetFieldTypeInfoResponse response = instantiateJerseyResponse(SchemaGetFieldTypeInfoResponse.class);

    final String realName = "fieldtypes";

    SimpleOrderedMap<Object> fieldTypeInfo = retrieveFieldInfoOfType(realName, fieldTypeName, params);
    if (fieldTypeInfo != null) {
      response.fieldTypeInfo = fieldTypeInfo;
      return response;
    }
    throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path /" + realName + "/" + fieldTypeName);
  }

  public static class SchemaGetFieldTypeInfoResponse extends SolrJerseyResponse {
    @JsonProperty("fieldType")
    public SimpleOrderedMap<?> fieldTypeInfo;
  }

  private Object listAllFieldsOfType(String realName, SolrParams params) {
    String camelCaseRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues =
      indexSchema.getNamedPropertyValues(realName, params);
    Object o = propertyValues.get(camelCaseRealName);
    if (params.getBool("meta", false)) {
      if (o instanceof NamedList) {
        @SuppressWarnings("unchecked")
        NamedList<Object> nl = (NamedList<Object>) o;
        String klas = (String) nl.get("class");
        PluginInfo.ClassName parsedClassName = new PluginInfo.ClassName(klas);
        if (parsedClassName.pkg != null) {
          SolrClassLoader solrClassLoader = indexSchema.getSolrClassLoader();
          MapWriter mw =
            solrClassLoader instanceof PackageListeningClassLoader
              ? ((PackageListeningClassLoader) solrClassLoader)
              .getPackageVersion(parsedClassName)
              : null;
          if (mw != null) {
            nl.add("_packageinfo_", mw);
          }
        }
      }
    }
    return o;
  }

  @SuppressWarnings("unchecked")
  private SimpleOrderedMap<Object> retrieveFieldInfoOfType(String realName, String fieldName, SolrParams params){
    SimpleOrderedMap<Object> returnFieldInfo = null;
    String camelCaseRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues =
      indexSchema.getNamedPropertyValues(realName, params);
    Object o = propertyValues.get(camelCaseRealName);
    if (o instanceof List) {
      List<?> list = (List<?>) o;
      for (Object obj : list) {
        if (obj instanceof SimpleOrderedMap) {
          SimpleOrderedMap<Object> fieldInfo = (SimpleOrderedMap<Object>) obj;
          if (fieldName.equals(fieldInfo.get("name"))) {
            returnFieldInfo = fieldInfo;
            if (params.getBool("meta", false)) {
              String klas = (String) fieldInfo.get("class");
              PluginInfo.ClassName parsedClassName = new PluginInfo.ClassName(klas);
              if (parsedClassName.pkg != null) {
                SolrClassLoader solrClassLoader = indexSchema.getSolrClassLoader();
                MapWriter mw =
                  solrClassLoader instanceof PackageListeningClassLoader
                    ? ((PackageListeningClassLoader) solrClassLoader)
                    .getPackageVersion(parsedClassName)
                    : null;
                if (mw != null) {
                  returnFieldInfo.add("_packageinfo_", mw);
                }
              }
            }
            break;
          }
        }
      }
    }
    return returnFieldInfo;
  }
}
