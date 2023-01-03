package com.flipkart.neo.solr.ltr.searcher.listener;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.cache.BannerCache;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import com.flipkart.neo.solr.ltr.schema.parser.StoredDocumentParser;
import com.flipkart.neo.solr.ltr.update.processor.NeoUpdateProcessorFactory;
import org.apache.lucene.document.Document;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NeoSolrEventListener extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String UPDATE_PROCESSOR = "updateProcessor";
  private static final Set<String> BANNER_ID_AND_VERSION_SET = new HashSet<>(Arrays.asList(SchemaFieldNames.BANNER_ID,
      SchemaFieldNames.VERSION));

  private static final int MAX_DELETED_BANNERS_IN_BANNER_CACHE = 10000;

  private String updateProcessorName;

  public NeoSolrEventListener(SolrCore core) {
    super(core);
  }

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    this.updateProcessorName = (String) args.get(UPDATE_PROCESSOR);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    log.warn("NeoSolrEventListener staring for newSearcher: {}, currentSearcher: {}", newSearcher, currentSearcher);
    if (StringUtils.isEmpty(updateProcessorName)) {
      log.warn("NeoSolrEventListener null.....");
    }
    NeoUpdateProcessorFactory neoUpdateProcessorFactory =
        (NeoUpdateProcessorFactory) newSearcher.getCore().getUpdateProcessors().get(updateProcessorName);
    if (newSearcher.getCore() == null) {
      log.warn("core is null.....");
    }
    if (neoUpdateProcessorFactory == null) {
      log.warn("neoUpdateProcessorFactory is null.....");
    }
    BannerCache bannerCache = neoUpdateProcessorFactory.getBannerCache();
    StoredDocumentParser documentParser = neoUpdateProcessorFactory.getStoredDocumentParser();
    String searcherBannerCacheName = neoUpdateProcessorFactory.getSearcherBannerCacheName();

    // null currentSearcher indicates it is very first searcher for collection.
    boolean firstSearcher = (currentSearcher == null);
    warmUpNewCacheWithAllBanners(newSearcher, searcherBannerCacheName, bannerCache, documentParser, firstSearcher);
    log.info("NeoSolrEventListener done.");
  }

  private void warmUpNewCacheWithAllBanners(SolrIndexSearcher newSearcher, String searcherBannerCacheName,
                                            BannerCache bannerCache, StoredDocumentParser documentParser,
                                            boolean firstSearcher) {
    @SuppressWarnings("unchecked")
    SolrCache<Integer, IndexedBanner> newSearcherBannerCache = newSearcher.getCache(searcherBannerCacheName);

    DocIterator docIterator;
    try {
      docIterator = newSearcher.getLiveDocSet().iterator();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

//    log.info("bannerCache preSize: {}", bannerCache.size());

    boolean bannerCacheCleanupRequired = isBannerCacheCleanupRequired(newSearcher, bannerCache);
    Set<String> bannerIdsToRetain = null;
    if (bannerCacheCleanupRequired) {
      bannerIdsToRetain = new HashSet<>(newSearcher.numDocs(), 1);
    }

    while (docIterator.hasNext()) {
      int newDocId = docIterator.nextDoc();
      String bannerId = null;
      try {
        Document document = newSearcher.doc(newDocId, BANNER_ID_AND_VERSION_SET);
        bannerId = documentParser.parseBannerId(document);

        if (bannerCacheCleanupRequired) {
          bannerIdsToRetain.add(bannerId);
        }

        if (firstSearcher) {
          handleFirstSearcher(newDocId, document, documentParser, bannerCache, newSearcherBannerCache);
        } else {
          handleNewSearcher(newDocId, document, documentParser, bannerId, bannerCache,newSearcherBannerCache);
        }
      } catch (Throwable e) {
        // catching everything for per doc operation as don't want failure for one doc to interfere with others.
        log.error("unable to read or parse banner, docId {}, bannerId {}", newDocId, bannerId, e);
      }
    }

    if (bannerCacheCleanupRequired) {
      // This may also remove some recently added entries from bannerCache, but successive searcher would put them back.
      // This invocation is supposed to be happening rarely, has been put as an auto-correct measure.
      cleanupBannerCache(bannerCache, bannerIdsToRetain);
    }

//    log.info("bannerCache postSize: {}", bannerCache.size());
  }

  /*
    In case of firstSearcher we parse stored document and put both in collection-level bannerCache and
    searcherBannerCache.
   */
  private void handleFirstSearcher(int docId, Document document, StoredDocumentParser documentParser,
                                   BannerCache bannerCache, SolrCache<Integer, IndexedBanner> searcherBannerCache)
      throws IOException {
    IndexedBanner banner = documentParser.parse(document);
    bannerCache.putBanner(banner);
    searcherBannerCache.put(docId, banner);
  }

  /*
    In case of newSearcher, first we get banner from bannerCache, if banner is found and its version also matches,
    we directly use it and put it in searcherBannerCache. Otherwise we recompute it and put it in both caches.
    Going this way instead of relying on update postProcessor to mitigate SOLR-8030 and any peer-sync that bypasses
    custom updateProcessor and invokes newSearcher instead of firstSearcher.

    We detect any updates and compute updated value and write back to bannerCache from here.
    Though it has two edge-cases -
    i) It may write back deleted entry from here as there is no way to differentiate between new addition and any
    deletion that is not still visible to this searcher. This may result in deleted documents getting accumulated over
    time. For this we do a cleanup once bannerCache size exceeds searcherNumDocs by some threshold value.
    ii) It may write back stale entry due to race condition between this and update call. But this would get fixed by
    subsequent searcher.

    There is a catch if a banner is re-indexed with same version (can happen in scenarios such as new fields being added
    to index and we are not re-creating the index, just updating all banners), it will not update the cache.
    So in order to update the caches, either a reload or restart or re-create or newVersion is must.
   */
  private void handleNewSearcher(int docId, Document document, StoredDocumentParser documentParser,
                                 String bannerId, BannerCache bannerCache,
                                 SolrCache<Integer, IndexedBanner> searcherBannerCache) throws IOException {
    IndexedBanner banner = bannerCache.getBanner(bannerId);
    if (banner == null || banner.getVersion() < documentParser.parseVersion(document)) {
      banner = documentParser.parse(document);
      bannerCache.putBanner(banner);
    }
    searcherBannerCache.put(docId, banner);
  }

  private boolean isBannerCacheCleanupRequired(SolrIndexSearcher newSearcher, BannerCache bannerCache) {
    // this is approx value as some recent additions may not be visible to this searcher.
    int approxDeletedBanners = bannerCache.size() - newSearcher.numDocs();
    log.info("approxDeletedBanners(-ve implies additions): {}", approxDeletedBanners);
    return approxDeletedBanners > MAX_DELETED_BANNERS_IN_BANNER_CACHE;
  }

  private void cleanupBannerCache(BannerCache bannerCache, Set<String> bannerIdsToRetain) {
//    log.info("doing bannerCache cleanup, bannerCacheSize:{}, bannerIdsToRetainSize:{}", bannerCache.size(),
//        bannerIdsToRetain.size());
    bannerCache.retainAllBanners(bannerIdsToRetain);
  }
}
