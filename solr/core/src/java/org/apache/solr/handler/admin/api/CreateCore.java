package org.apache.solr.handler.admin.api;

import com.ctc.wstx.util.StringUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.api.endpoint.CreateCoreApi;
import org.apache.solr.client.api.model.CreateCoreRequestBody;
import org.apache.solr.client.api.model.CreateCoreResponseBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.PropertiesUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

import javax.inject.Inject;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.handler.api.V2ApiUtils.flattenToCommaDelimitedString;

public class CreateCore extends CoreAdminAPIBase implements CreateCoreApi {
    private ObjectMapper objectMapper;
    @Inject
    public CreateCore(CoreContainer coreContainer, CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker, SolrQueryRequest req, SolrQueryResponse rsp) {
        super(coreContainer, coreAdminAsyncTracker, req, rsp);
        this.objectMapper = SolrJacksonMapper.getObjectMapper();
    }

    @Override
    @PermissionName(PermissionNameProvider.Name.CORE_EDIT_PERM)
    public SolrJerseyResponse createCore(CreateCoreRequestBody createCoreRequestBody) {
        var requestBodyMap = objectMapper.convertValue(createCoreRequestBody, HashMap.class);
        if(createCoreRequestBody.isTransient != null){
            requestBodyMap.put("transient", requestBodyMap.remove("isTransient"));
        }
        if(createCoreRequestBody.properties != null){
            requestBodyMap.remove("properties");
            flattenMapWithPrefix(createCoreRequestBody.properties, requestBodyMap, PROPERTY_PREFIX);
        }
        if(createCoreRequestBody.roles != null && !createCoreRequestBody.roles.isEmpty()){
            requestBodyMap.remove("roles");
            flattenToCommaDelimitedString(requestBodyMap, createCoreRequestBody.roles, "roles");
        }
        return createCore(requestBodyMap, createCoreRequestBody.name);
    }

    public SolrJerseyResponse createCore(Map<String, Object> params, String coreName){
        var response = instantiateJerseyResponse(CreateCoreResponseBody.class);
        Map<String, String> coreParams = buildCoreParams(params);
        Path instancePath;

        // TODO: Should we nuke setting odd instance paths?  They break core discovery, generally
        String instanceDir = String.valueOf(params.get(CoreAdminParams.INSTANCE_DIR));
        if (instanceDir == null) instanceDir = (String) params.get("property.instanceDir");
        if (instanceDir != null) {
            instanceDir =
                    PropertiesUtil.substituteProperty(
                            instanceDir, coreContainer.getContainerProperties());
            instancePath = coreContainer.getCoreRootDirectory().resolve(instanceDir).normalize();
        } else {
            instancePath = coreContainer.getCoreRootDirectory().resolve(coreName);
        }

        boolean newCollection = params.get(CoreAdminParams.NEW_COLLECTION) != null ?
                StrUtils.parseBool(params.get(CoreAdminParams.NEW_COLLECTION).toString()): false;

        coreContainer.create(coreName, instancePath, coreParams, newCollection);

        response.core = coreName;
        return response;
    }
    public static final Map<String, String> buildCoreParams(Map<String, Object> params) {
        Map<String, String> coreParams = new HashMap<>();

        // standard core create parameters
        for (Map.Entry<String, String> entry : CoreAdminHandler.paramToProp.entrySet()) {
            String value = String.valueOf(params.getOrDefault(entry.getKey(), null));
            if (StrUtils.isNotNullOrEmpty(value)) {
                coreParams.put(entry.getValue(), value);
            }
        }

        // extra properties
        params.forEach((param, propValue) -> {
            if (param.startsWith(CoreAdminParams.PROPERTY_PREFIX)) {
                String propName = param.substring(CoreAdminParams.PROPERTY_PREFIX.length());
                coreParams.put(propName, String.valueOf(propValue));
            }
            if (param.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
                coreParams.put(param, String.valueOf(params.get(param)));
            }
        });

        return coreParams;
    }
}
