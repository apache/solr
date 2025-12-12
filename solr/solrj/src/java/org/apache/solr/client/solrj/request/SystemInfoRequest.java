package org.apache.solr.client.solrj.request;


import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Class to get a "/admin/info/system" response.
 */
public class SystemInfoRequest extends SolrRequest<SystemInfoResponse> {

  private static final long serialVersionUID = 1L;
  
  public SystemInfoRequest() {
    super(METHOD.GET, CommonParams.SYSTEM_INFO_PATH, SolrRequestType.ADMIN);
  }

  public SystemInfoRequest(String path) {
    super(METHOD.GET, path, SolrRequestType.ADMIN);
  }

  @Override
  public SolrParams getParams() {
    return null; // no params to return
  }

  @Override
  protected SystemInfoResponse createResponse(NamedList<Object> namedList) {
    return (SystemInfoResponse) namedList.get("response");
  }

}
