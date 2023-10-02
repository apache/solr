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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrClassLoader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.pkg.PackageListeningClassLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

/**
 * <code>GetSchemaFieldAPI</code> contains the V2 APIs for all field related endpoint which are
 *
 * <ul>
 *   <li>/fields
 *   <li>/fields/{fieldName}
 *   <li>/copyfields
 *   <li>/dynamicfields
 *   <li>/dynamicfields/{fieldName}
 *   <li>/fieldtypes
 *   <li>/fieldtypes/{fieldTypeName}
 * </ul>
 */
@Path("/{a:cores|collections}/{collectionName}/schema")
public class GetSchemaFieldAPI /*extends GetSchemaAPI*/ extends JerseyResource {

  private final IndexSchema indexSchema;
  private final SolrParams params;

  // TODO Stop using SolrParams here and instead give API methods parameters representing only those
  // query-params that they support
  @Inject
  public GetSchemaFieldAPI(IndexSchema indexSchema, SolrParams params) {
    this.indexSchema = indexSchema;
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
    public List<Object> fields;
  }

  @GET
  @Path("/fields/{fieldName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetFieldInfoResponse getFieldInfo(@PathParam("fieldName") String fieldName) {
    if (fieldName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field name must not be null");
    }
    SchemaGetFieldInfoResponse response =
        instantiateJerseyResponse(SchemaGetFieldInfoResponse.class);
    final String realName = "fields";

    SimpleOrderedMap<Object> fieldInfo = retrieveFieldInfoOfType(realName, fieldName, params);
    if (fieldInfo != null) {
      response.fieldInfo = fieldInfo;
      return response;
    }
    throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such field [" + fieldName + "]");
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
    SchemaListCopyFieldsResponse response =
        instantiateJerseyResponse(SchemaListCopyFieldsResponse.class);
    final String realName = "copyfields";

    response.copyFields = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListCopyFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("copyFields")
    public List<Object> copyFields;
  }

  @GET
  @Path("/dynamicfields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListDynamicFieldsResponse listDynamicFields() {
    SchemaListDynamicFieldsResponse response =
        instantiateJerseyResponse(SchemaListDynamicFieldsResponse.class);
    final String realName = "dynamicfields";

    response.dynamicFields = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListDynamicFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("dynamicFields")
    public List<Object> dynamicFields;
  }

  @GET
  @Path("/dynamicfields/{fieldName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetDynamicFieldInfoResponse getDynamicFieldInfo(
      @PathParam("fieldName") String fieldName) {
    if (fieldName == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Dynamic field name must not be null");
    }
    SchemaGetDynamicFieldInfoResponse response =
        instantiateJerseyResponse(SchemaGetDynamicFieldInfoResponse.class);
    final String realName = "dynamicfields";

    SimpleOrderedMap<Object> dynamicFieldInfo =
        retrieveFieldInfoOfType(realName, fieldName, params);
    if (dynamicFieldInfo != null) {
      response.dynamicFieldInfo = dynamicFieldInfo;
      return response;
    }
    throw new SolrException(
        SolrException.ErrorCode.NOT_FOUND, "No such dynamic field [" + fieldName + "]");
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
    SchemaListFieldTypesResponse response =
        instantiateJerseyResponse(SchemaListFieldTypesResponse.class);
    final String realName = "fieldtypes";

    response.fieldTypes = listAllFieldsOfType(realName, params);

    return response;
  }

  public static class SchemaListFieldTypesResponse extends SolrJerseyResponse {
    @JsonProperty("fieldTypes")
    public List<Object> fieldTypes;
  }

  @GET
  @Path("/fieldtypes/{fieldTypeName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaGetFieldTypeInfoResponse getFieldTypeInfo(
      @PathParam("fieldTypeName") String fieldTypeName) {
    if (fieldTypeName == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Field type name must not be null");
    }
    SchemaGetFieldTypeInfoResponse response =
        instantiateJerseyResponse(SchemaGetFieldTypeInfoResponse.class);

    final String realName = "fieldtypes";

    SimpleOrderedMap<Object> fieldTypeInfo =
        retrieveFieldInfoOfType(realName, fieldTypeName, params);
    if (fieldTypeInfo != null) {
      response.fieldTypeInfo = fieldTypeInfo;
      return response;
    }
    throw new SolrException(
        SolrException.ErrorCode.NOT_FOUND, "No such field type [" + fieldTypeName + "]");
  }

  public static class SchemaGetFieldTypeInfoResponse extends SolrJerseyResponse {
    @JsonProperty("fieldType")
    public SimpleOrderedMap<?> fieldTypeInfo;
  }

  private List<Object> listAllFieldsOfType(String realName, SolrParams params) {
    String camelCaseRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues = indexSchema.getNamedPropertyValues(realName, params);
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) propertyValues.get(camelCaseRealName);
    if (params.getBool("meta", false)) {
      insertPackageInfo(list);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private SimpleOrderedMap<Object> retrieveFieldInfoOfType(
      String realName, String fieldName, SolrParams params) {
    SimpleOrderedMap<Object> returnFieldInfo = null;
    String camelCaseRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues = indexSchema.getNamedPropertyValues(realName, params);
    Object o = propertyValues.get(camelCaseRealName);
    if (o instanceof List) {
      List<?> list = (List<?>) o;
      for (Object obj : list) {
        if (obj instanceof SimpleOrderedMap) {
          SimpleOrderedMap<Object> fieldInfo = (SimpleOrderedMap<Object>) obj;
          if (fieldName.equals(fieldInfo.get("name"))) {
            returnFieldInfo = fieldInfo;
            if (params.getBool("meta", false)) {
              insertPackageInfo(returnFieldInfo);
            }
            break;
          }
        }
      }
    }
    return returnFieldInfo;
  }

  /**
   * If a plugin is loaded from a package, the version of the package being used should be added to
   * the response
   */
  private void insertPackageInfo(Object o) {
    if (o instanceof List) {
      List<?> l = (List<?>) o;
      for (Object o1 : l) {
        if (o1 instanceof NamedList || o1 instanceof List) insertPackageInfo(o1);
      }

    } else if (o instanceof NamedList) {
      @SuppressWarnings("unchecked")
      NamedList<Object> nl = (NamedList<Object>) o;
      nl.forEach(
          (n, v) -> {
            if (v instanceof NamedList || v instanceof List) insertPackageInfo(v);
          });
      Object v = nl.get("class");
      if (v instanceof String) {
        String klas = (String) v;
        PluginInfo.ClassName parsedClassName = new PluginInfo.ClassName(klas);
        if (parsedClassName.pkg != null) {
          SolrClassLoader solrClassLoader = indexSchema.getSolrClassLoader();
          MapWriter mw =
              solrClassLoader instanceof PackageListeningClassLoader
                  ? ((PackageListeningClassLoader) solrClassLoader)
                      .getPackageVersion(parsedClassName)
                  : null;
          if (mw != null) nl.add("_packageinfo_", mw);
        }
      }
    }
  }
}
