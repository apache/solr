package org.apache.solr.search;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.RefCounted;

import java.io.IOException;
import java.util.List;

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
                final List<String> loadedCoreNames = coreContainer.getLoadedCoreNames();
                String rightSideCore = req.getCore().getName();
                for (String leftCoreName: loadedCoreNames){
                    if (!leftCoreName.equals(rightSideCore)) {
                        final SolrCore core = coreContainer.getCore(leftCoreName);
                        final RefCounted<SolrIndexSearcher> leftSearcher = core.getSearcher();
                        try {
                            @SuppressWarnings("rawtypes")
                            final SolrCache joinCache = leftSearcher.get().getCache(rightSideCore);
                            if (joinCache != null) {
                                joinCache.warm(leftSearcher.get(), joinCache);
                            }
                        } finally {
                            leftSearcher.decref();
                            core.close();
                        }
                    }
                }
            }
        };
    }
}
