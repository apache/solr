package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ParamSetsApi;
import org.apache.solr.client.api.model.ParamSetsResponse;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RequestParams;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;

public class GetParamSets extends JerseyResource implements ParamSetsApi.Get {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public GetParamSets(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ParamSetsResponse getParamSets() {
    return buildResponse(null);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ParamSetsResponse getParamSets(String paramSetName) {
    return buildResponse(paramSetName);
  }

  @SuppressWarnings("unchecked")
  private ParamSetsResponse buildResponse(String paramSetName) {
    final var response = instantiateJerseyResponse(ParamSetsResponse.class);
    final RequestParams requestParams = solrQueryRequest.getCore().getSolrConfig().getRequestParams();
    response.znodeVersion = requestParams.getZnodeVersion();

    if (paramSetName == null) {
      Map<String, Object> paramsMap = Utils.convertToMap(requestParams, new LinkedHashMap<>());
      response.params = (Map<String, Object>) paramsMap.get(RequestParams.NAME);
    } else {
      RequestParams.ParamSet paramSet = requestParams.getParams(paramSetName);
      // throw 4xx if this null
      if (paramSet != null) {
        Map<String, Object> paramsMap = new LinkedHashMap<>();
        paramsMap.put(paramSetName, Utils.convertToMap(paramSet, new LinkedHashMap<>()));
        response.params = paramsMap;
      }
    }
    return response;
  }
}
