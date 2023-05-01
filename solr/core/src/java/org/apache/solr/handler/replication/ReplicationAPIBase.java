package org.apache.solr.handler.replication;

import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.io.IOException;

/** A common parent for "replication" (i.e. replication-level) APIs. */
public abstract class ReplicationAPIBase extends JerseyResource {

    protected final CoreContainer coreContainer;
    protected final SolrQueryRequest solrQueryRequest;
    protected final SolrQueryResponse solrQueryResponse;

    public ReplicationAPIBase(
        CoreContainer coreContainer,
        SolrQueryRequest solrQueryRequest,
        SolrQueryResponse solrQueryResponse) {
        this.coreContainer = coreContainer;
        this.solrQueryRequest = solrQueryRequest;
        this.solrQueryResponse = solrQueryResponse;
    }

    protected void fetchIndexVersion(String coreName, CoreReplicationAPI.GetIndexResponse rsp) throws IOException {

        if(coreContainer.getCore(coreName) == null) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    String.format("Solr core %s does not exist", coreName));
        }

        ReplicationHandler replicationHandler = (ReplicationHandler) coreContainer
                .getCore(coreName)
                .getRequestHandler(ReplicationHandler.PATH);

        replicationHandler.getIndexVersionResponse(rsp);

    }



}
