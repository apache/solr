package com.flipkart.neo.solr.ltr.update.processor;

import java.io.IOException;

import com.flipkart.neo.solr.ltr.banner.cache.BannerCache;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

public class NeoUpdateProcessor extends UpdateRequestProcessor {
  private static final String NEO_PROCESS_DELETE = "neoProcessDelete";

  private final BannerCache bannerCache;

  private final MetricPublisher metricPublisher;

  NeoUpdateProcessor(UpdateRequestProcessor next, BannerCache bannerCache, MetricPublisher metricPublisher) {
    super(next);
    this.bannerCache = bannerCache;
    this.metricPublisher = metricPublisher;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    // In light of SOLR-8030 and any peer-sync that bypasses custom updateProcessor and invokes newSearcher
    // instead of firstSearcher, not doing bannerCache update from here as it can not be always relied upon.
    // All bannerCache updates are now done during searcher warmup.
    // In the hindsight, not doing bannerCache update here makes update call less expensive as bannerCache update is
    // totally offline now.
    super.processAdd(cmd);
  }

  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    long neoProcessDeleteStartTime = System.currentTimeMillis();
    // Delete from here is on best effort basis. To handle the cases where it is bypassed due to SOLR-8030 issue or
    // any peer-sync that bypasses it, a cleanup is invoked while searcher warmup once deleted banners cross certain
    // threshold. Not getting rid of deletion from here as cleanup is costly and should be invoked rarely.
    bannerCache.deleteBanner(cmd.getId());
    metricPublisher.updateTimer(NEO_PROCESS_DELETE, System.currentTimeMillis() - neoProcessDeleteStartTime);

    super.processDelete(cmd);
  }
}
