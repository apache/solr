package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.mockito.Mockito;

import java.util.ArrayList;

public class MockShardRequest extends ShardRequest {

    public static MockShardRequest create() {
        MockShardRequest mockShardRequest = new MockShardRequest();
        mockShardRequest.responses = new ArrayList<>();
        return mockShardRequest;
    }

    public MockShardRequest withShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
        ShardResponse shardResponse = buildShardResponse(responseHeader, solrDocuments);
        responses.add(shardResponse);
        return this;
    }

    private ShardResponse buildShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
        SolrResponse solrResponse = Mockito.mock(SolrResponse.class);
        ShardResponse shardResponse = new ShardResponse();
        NamedList<Object> response = new NamedList<>();
        response.add("response", solrDocuments);
        shardResponse.setSolrResponse(solrResponse);
        response.add("responseHeader", responseHeader);
        Mockito.when(solrResponse.getResponse()).thenReturn(response);

        return shardResponse;
    }

}
