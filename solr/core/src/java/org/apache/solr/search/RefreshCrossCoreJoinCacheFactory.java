package org.apache.solr.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.RefCounted;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;

/**
 *  This update processor is expected to be invoked on "fromIndex" side of join to regenerate cached join.
 *  It loops through all other cores checking them if they are "toIndex" cores:
 *  those "toIndex" cores, which have user cache with name of this core ("fromIndex") are subj of regeneration.
 *
 * */
public class RefreshCrossCoreJoinCacheFactory extends UpdateRequestProcessorFactory {
    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new UpdateRequestProcessor(next) {
            @SuppressWarnings("unchecked")
            @Override
            public void processCommit(CommitUpdateCommand cmd) throws IOException {
                super.processCommit(cmd);
                // refresh strictly after RunUpdateProcessor

                final CoreContainer coreContainer = req.getCore().getCoreContainer();
                final List<String> possibleToSideCores = coreContainer.getLoadedCoreNames();
                String fromSideCore = req.getCore().getName();
                for (String toSideCoreName: possibleToSideCores){
                    if (!toSideCoreName.equals(fromSideCore)) {
                        final SolrCore toSideCore = coreContainer.getCore(toSideCoreName);
                        final RefCounted<SolrIndexSearcher> toSideSearcher = toSideCore.getSearcher();
                        try {
                            @SuppressWarnings("rawtypes")
                            final SolrCache joinCache = toSideSearcher.get().getCache(fromSideCore);
                            if (joinCache != null) {
                                // this is necessary for classic join query, which checks SRI, I don't know why.
                                SolrQueryRequest leftReq = new LocalSolrQueryRequest(toSideCore,req.getParams()) {
                                    @Override public SolrIndexSearcher getSearcher() { return toSideSearcher.get(); }
                                    @Override public void close() { }
                                };
                                SolrQueryResponse rsp = new SolrQueryResponse();
                                SolrRequestInfo.setRequestInfo(new SolrRequestInfo(leftReq, rsp));
                                try {
                                    joinCache.warm(toSideSearcher.get(), joinCache);
                                } finally {
                                    SolrRequestInfo.clearRequestInfo();
                                }
                            }
                        } finally {
                            toSideSearcher.decref();
                            toSideCore.close();
                        }
                    }
                }
            }
        };
    }
}
