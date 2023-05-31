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
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrClassLoader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.pkg.PackageListeningClassLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

@Path("/{a:cores|collections}/{collectionName}/schema")
public class GetSchemaAPI extends JerseyResource {

  private IndexSchema indexSchema;

  @Inject
  public GetSchemaAPI(IndexSchema indexSchema) {
    this.indexSchema = indexSchema;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaInfoResponse getSchemaInfo() {
    final var response = instantiateJerseyResponse(SchemaInfoResponse.class);

    response.schema = indexSchema.getNamedPropertyValues();

    return response;
  }

  public static class SchemaInfoResponse extends SolrJerseyResponse {
    // TODO The schema response is quite complicated, so for the moment it's sufficient to record it
    // here only as a Map.  However, if SOLR-16825 is tackled then there will be a lot of value in
    // describing this response format more accurately so that clients can navigate the contents
    // without lots of map fetching and casting.
    @JsonProperty("schema")
    public Map<String, Object> schema;
  }

  @GET
  @Path("/similarity")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaSimilarityResponse getSchemaSimilarity() {
    final var response = instantiateJerseyResponse(SchemaSimilarityResponse.class);

    response.similarity = indexSchema.getSimilarityFactory().getNamedPropertyValues();

    return response;
  }

  public static class SchemaSimilarityResponse extends SolrJerseyResponse {
    // TODO The schema response is quite complicated, so for the moment it's sufficient to record it
    // here only as a Map.  However, if SOLR-16825 is tackled then there will be a lot of value in
    // describing this response format more accurately so that clients can navigate the contents
    // without lots of map fetching and casting.
    @JsonProperty("similarity")
    public SimpleOrderedMap<Object> similarity;
  }

  @GET
  @Path("/uniquekey")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaUniqueKeyResponse getSchemaUniqueKey() {
    final var response = instantiateJerseyResponse(SchemaUniqueKeyResponse.class);

    response.uniqueKey = indexSchema.getUniqueKeyField().getName();

    return response;
  }

  public static class SchemaUniqueKeyResponse extends SolrJerseyResponse {
    @JsonProperty("uniqueKey")
    public String uniqueKey;
  }

  @GET
  @Path("/version")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaVersionResponse getSchemaVersion() {
    final var response = instantiateJerseyResponse(SchemaVersionResponse.class);

    response.version = indexSchema.getVersion();

    return response;
  }

  public static class SchemaVersionResponse extends SolrJerseyResponse {
    @JsonProperty("version")
    public float version;
  }

  @GET
  @Path("/fields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListFieldsResponse listSchemaFields(SchemaGetFieldRequestBody requestBody) {
    SchemaListFieldsResponse response = instantiateJerseyResponse(SchemaListFieldsResponse.class);
    final String realName = "fields";

    response.fields = listAllFieldsOfType(realName, requestBody);

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
  public SchemaGetFieldInfoResponse getFieldInfo(@PathParam("fieldName") String fieldName, SchemaGetFieldRequestBody requestBody) throws Exception {
    SchemaGetFieldInfoResponse response = instantiateJerseyResponse(SchemaGetFieldInfoResponse.class);
    final String realName = "fields";

    SimpleOrderedMap<Object> fieldInfo = retrieveFieldInfoOfType(realName, fieldName, requestBody);
    if (fieldInfo != null) {
      response.fieldInfo = fieldInfo;
      return response;
    }
    throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path /fields/" + fieldName);
  }

  public static class SchemaGetFieldInfoResponse extends SolrJerseyResponse {
    @JsonProperty("field")
    public SimpleOrderedMap<?> fieldInfo;
  }

  @GET
  @Path("/copyfields")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaListCopyFieldsResponse listCopyFields(SchemaGetFieldRequestBody requestBody) {
    SchemaListCopyFieldsResponse response = instantiateJerseyResponse(SchemaListCopyFieldsResponse.class);
    final String realName = "copyfields";

    response.copyFields = listAllFieldsOfType(realName, requestBody);

    return response;
  }


  public static class SchemaListCopyFieldsResponse extends SolrJerseyResponse {
    @JsonProperty("copyFields")
    public Object copyFields;
  }

  public static class SchemaGetFieldRequestBody implements JacksonReflectMapWriter {
    @JsonProperty("params")
    public SolrParams params;

    public SchemaGetFieldRequestBody(SolrParams params) {
      this.params = params;
    }
  }
  private Object listAllFieldsOfType(String realName, SchemaGetFieldRequestBody requestBody) {
    String singularRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues =
      indexSchema.getNamedPropertyValues(realName, requestBody.params);
    Object o = propertyValues.get(singularRealName);
    if (requestBody.params.getBool("meta", false)) {
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
  private SimpleOrderedMap<Object> retrieveFieldInfoOfType(String realName, String fieldName, SchemaGetFieldRequestBody requestBody){
    SimpleOrderedMap<Object> fieldInfo = null;
    String singularRealName = IndexSchema.nameMapping.get(realName);
    Map<String, Object> propertyValues =
      indexSchema.getNamedPropertyValues(realName, requestBody.params);
    Object o = propertyValues.get(singularRealName);
    if (o instanceof List) {
      List<?> list = (List<?>) o;
      for (Object obj : list) {
        if (obj instanceof SimpleOrderedMap) {
          fieldInfo = (SimpleOrderedMap<Object>) obj;
          if (fieldName.equals(fieldInfo.get("name"))) {
            if (requestBody.params.getBool("meta", false)) {
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
                  fieldInfo.add("_packageinfo_", mw);
                }
              }
            }
            break;
          }
        }
      }
    }
    return fieldInfo;
  }

}
