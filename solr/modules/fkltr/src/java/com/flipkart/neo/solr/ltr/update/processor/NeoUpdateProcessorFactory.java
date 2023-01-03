package com.flipkart.neo.solr.ltr.update.processor;

import java.lang.invoke.MethodHandles;
import java.util.Timer;
import java.util.TimerTask;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.kloud.authn.AuthToken;
import com.flipkart.neo.content.ranking.model.ActiveModelsRetriever;
import com.flipkart.neo.content.ranking.model.DynamicBucketActiveModelRetriever;
import com.flipkart.neo.content.ranking.model.ModelManager;
import com.flipkart.ads.model.entities.ModelMysqlConfig;
import com.flipkart.kloud.authn.AuthTokenService;
import com.flipkart.kloud.config.ConfigClient;
import com.flipkart.kloud.config.ConfigClientBuilder;
import com.flipkart.kloud.config.DynamicBucket;
import com.flipkart.kloud.config.error.ConfigServiceException;
import com.flipkart.neo.solr.ltr.banner.cache.BannerCache;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.neo.solr.ltr.constants.DynamicConfigBucketKeys;
import com.flipkart.neo.solr.ltr.constants.StaticConfigBucketKeys;
import com.flipkart.neo.solr.ltr.module.NeoScoringModule;
import com.flipkart.neo.solr.ltr.module.NeoScoringModuleImpl;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.schema.parser.StoredDocumentParser;
import com.flipkart.neo.solr.ltr.scorer.NeoDocumentScorer;
import com.flipkart.neo.solr.ltr.scorer.allocator.ExploringL1DocumentAllocator;
import com.flipkart.neo.solr.ltr.scorer.allocator.L1DocumentAllocator;
import com.flipkart.neo.solr.ltr.search.NeoLTRQParserPlugin;
import com.flipkart.neo.solr.ltr.transformers.NeoScoreMetaSerializer;
import com.flipkart.neo.solr.ltr.transformers.NeoScoreMetaTransformerFactory;
import com.flipkart.security.cryptex.CryptexClient;
import com.flipkart.security.cryptex.CryptexClientBuilder;
import com.flipkart.solr.ltr.scorer.FKLTRDocumentScorer;
import com.flipkart.solr.ltr.utils.ConfigBucketUtils;
import com.flipkart.solr.ltr.utils.FileUtils;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import com.flipkart.solr.ltr.utils.Parallelizer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flipkart.neo.solr.ltr.constants.NeoFKLTRConstants.AUTHN_CLIENT_SECRET_PATH;
import static com.flipkart.solr.ltr.constants.FKLTRConstants.METRICS_PREFIX;

public class NeoUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String NEO = "neo";
  private static final String BANNER_CACHE_SIZE_METRIC_NAME = "bannerCacheSize";

  // SolrConfig variable names
  private static final String FKLTR_PARSER_NAME = "fkltrParserName";
  private static final String SCORE_META_TRANSFORMER_FACTORY_NAME = "scoreMetaTransformerFactoryName";
  private static final String SEARCHER_BANNER_CACHE_NAME = "searcherBannerCacheName";
  private static final String BANNER_CACHE_INITIAL_SIZE = "bannerCacheInitialSize";
  private static final String AUTHN_CLIENT_ID = "authnClientId";
  private static final String STATIC_CONFIG_BUCKET_NAME = "staticConfigBucketName";
  private static final String DYNAMIC_CONFIG_BUCKET_NAME = "dynamicConfigBucketName";

  // Variables getting initialized during init using solrConfig
  private String fkltrParserName;
  private String scoreMetaTransformerFactoryName;
  private String searcherBannerCacheName;
  private int bannerCacheInitialSize;
  private String authnClientId;
  private String staticConfigBucketName;
  private String dynamicConfigBucketName;

  private NeoScoringModule neoScoringModule;
  private MetricPublisher metricPublisher;
  private StoredDocumentParser storedDocumentParser;

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    this.fkltrParserName = (String) args.get(FKLTR_PARSER_NAME);
    this.scoreMetaTransformerFactoryName = (String) args.get(SCORE_META_TRANSFORMER_FACTORY_NAME);

    this.searcherBannerCacheName = (String) args.get(SEARCHER_BANNER_CACHE_NAME);
    this.bannerCacheInitialSize = (int) args.get(BANNER_CACHE_INITIAL_SIZE);

    this.authnClientId = (String) args.get(AUTHN_CLIENT_ID);

    this.staticConfigBucketName = (String) args.get(STATIC_CONFIG_BUCKET_NAME);
    this.dynamicConfigBucketName = (String) args.get(DYNAMIC_CONFIG_BUCKET_NAME);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new NeoUpdateProcessor(next, neoScoringModule.getBannerCache(), metricPublisher);
  }

  @Override
  public void inform(SolrCore core) {
    log.warn("initialising neo collection");

    // setting up objectMapper
    ObjectMapper objectMapper = createObjectMapper();

    // setting up configBuckets
    AuthTokenService authTokenService = createAuthTokenService();
    log.warn("started AuthTokenService...");
    CryptexClient cryptexClient = createCryptexClient(authTokenService);
    log.warn("got CryptexClient...");
    DynamicBucket staticConfigBucket = getConfigBucket(staticConfigBucketName, cryptexClient);
    log.warn("read static bucket...");
    DynamicBucket dynamicConfigBucket = getConfigBucket(dynamicConfigBucketName, cryptexClient);
    log.warn("read dynami bucket...");
    // setting up neoFKLTRConfigs for dynamic configs
    NeoFKLTRConfigs neoFKLTRConfigs = new NeoFKLTRConfigs(dynamicConfigBucket);

    // setting up metricPublisher
    metricPublisher = new MetricPublisher(core.getCoreMetricManager().getRegistry(), METRICS_PREFIX);

    // initializing neoScoringModule
    log.warn("getting modelMySqlConfig...");
    ModelMysqlConfig modelMysqlConfig = getModelMysqlConfig(staticConfigBucket);
    boolean isStaticModelsEnabled = ConfigBucketUtils.getBoolean(staticConfigBucket, StaticConfigBucketKeys.STATIC_MODEL_ENABLED, false);
    log.warn("getting isStaticModelsEnabled... {}", isStaticModelsEnabled);
    int modelRefreshPeriodInSeconds = staticConfigBucket.getInt(StaticConfigBucketKeys.MODEL_REFRESH_PERIOD_IN_SECS);
    log.warn("getting modelRefreshPeriodInSeconds... {}", modelRefreshPeriodInSeconds);
    neoScoringModule = new NeoScoringModuleImpl(modelMysqlConfig, getActiveModelsRetriever(dynamicConfigBucket),
        modelRefreshPeriodInSeconds, bannerCacheInitialSize, neoFKLTRConfigs, core.getCoreMetricManager().getRegistry(), isStaticModelsEnabled);
    neoScoringModule.initialize();

    // registering metrics gauge for bannerCache size.
    metricPublisher.register(BANNER_CACHE_SIZE_METRIC_NAME, (Gauge<Integer>) () -> neoScoringModule.getBannerCache().size());

    // setting up document parsers
    storedDocumentParser = new StoredDocumentParser(neoScoringModule.getBannerScorer(), objectMapper);

    // setting up fkltrDocumentScorer
    int parallelizerNumThreads = staticConfigBucket.getInt(StaticConfigBucketKeys.PARALLELIZER_NUM_THREADS);
    FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> fkltrDocumentScorer = createFKLTRDocumentScorer(core,
        neoScoringModule.getModelManager(), parallelizerNumThreads, neoFKLTRConfigs);

    // setting up fkltr QParserPlugin
    NeoLTRQParserPlugin queryPlugin = (NeoLTRQParserPlugin) core.getQueryPlugin(fkltrParserName);
    queryPlugin.setMapper(objectMapper);
    queryPlugin.setFkltrDocumentScorer(fkltrDocumentScorer);

    // setting up scoreMetaTransformerFactory
    NeoScoreMetaTransformerFactory transformerFactory =
        (NeoScoreMetaTransformerFactory) core.getTransformerFactory(scoreMetaTransformerFactoryName);
    transformerFactory.setScoreMetaSerializer(new NeoScoreMetaSerializer(objectMapper));

    // cleanup post collection closeUp cron
    startSchedulerForCleanupPostCollectionCloseUp(core);

    log.info("neo collection initialisation done");
  }

  public BannerCache getBannerCache() {
    return neoScoringModule.getBannerCache();
  }

  public String getSearcherBannerCacheName() {
    return searcherBannerCacheName;
  }

  public StoredDocumentParser getStoredDocumentParser() {
    return storedDocumentParser;
  }

  private ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return objectMapper;
  }

  private CryptexClient createCryptexClient(AuthTokenService authTokenService) {
    CryptexClient c = null;
    try {
      c = new CryptexClientBuilder(authTokenService).build();
      log.warn("CryptexClientBuilder ran fine..., {}", c);
    } catch (Exception e ){
      e.printStackTrace();
      log.warn("Auth fetch token failed..., {}", (Object[]) e.getStackTrace());
    }
    return c;
  }

  private AuthTokenService createAuthTokenService() {
    // Discussed with authn team, they would be providing authn client with self discovery of endpoint base on host
    // metadata. Can't keep in config-bucket as config-service itself is behind authn now after cryptex.
    // Till then hardcoding ch endpoint, should'nt be a problem as only used by static bucket.
    log.warn("Auth after init : {}", this.authnClientId);
    log.warn("Auth after readStringFromFile : {}", FileUtils.readStringFromFile(AUTHN_CLIENT_SECRET_PATH));
    AuthTokenService.init("https://authn.ch.flipkart.com",
        this.authnClientId, FileUtils.readStringFromFile(AUTHN_CLIENT_SECRET_PATH));
    AuthTokenService authTokenService = AuthTokenService.getInstance();
//    log.warn("Auth is Initialised : {}", authTokenService.isInitialized());
    log.warn("Auth fetching token().... {}", authTokenService);

    AuthToken authToken;
    try {
      authToken = authTokenService.fetchToken("fk-neo-solr");
      log.warn("Auth fetch token() : {}", authToken.getToken());
      log.warn("Auth token expires in() : {}", authToken.getExpiresIn());
      log.warn("Auth token has expires in() : {}", authToken.hasExpired());
      log.warn("Auth token has expiryTime() : {}", authToken.getExpiryTime());
    } catch (Exception e) {
      e.printStackTrace();
      log.warn("Auth fetch token failed..., {}", (Object[]) e.getStackTrace());


    }


    return authTokenService;
  }

  private FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> createFKLTRDocumentScorer(
      SolrCore core,
      ModelManager modelManager,
      int parallelizerNumThreads,
      NeoFKLTRConfigs neoFKLTRConfigs
  ) {
    L1DocumentAllocator l1DocumentAllocator = new ExploringL1DocumentAllocator();
    Parallelizer parallelizer = new Parallelizer(parallelizerNumThreads, core.getCoreMetricManager().getRegistry(), NEO);

    return new NeoDocumentScorer(searcherBannerCacheName, parallelizer, l1DocumentAllocator,
        neoScoringModule.getBannerScorer(), modelManager, neoFKLTRConfigs, metricPublisher);
  }

  // todo:fkltr There has to be a better way to do this. But, alas! there does not seem to be a way in which
  //  it can be communicated that core is closed now.
  private void startSchedulerForCleanupPostCollectionCloseUp(SolrCore core) {
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        if (core.isClosed()) {
          // triggers cleanup of neoScoringModule.
          neoScoringModule.cleanup();
          // cancels itself, it can die in peace now.
          timer.cancel();
        }
      }
    }, 60000, 60000);
  }

  private ModelMysqlConfig getModelMysqlConfig(DynamicBucket configBucket) {
    ModelMysqlConfig modelMysqlConfig = new ModelMysqlConfig();
    log.warn("MODEL_MYSQL_APP_NAME {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_APP_NAME));
    modelMysqlConfig.setAppName(configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_APP_NAME));
    log.warn("MODEL_MYSQL_HOST {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_HOST));
    modelMysqlConfig.setMysqlModelClientHost(configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_HOST));
    log.warn("MODEL_MYSQL_PORT {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_PORT));
    modelMysqlConfig.setMysqlModelClientPort(configBucket.getInt(StaticConfigBucketKeys.MODEL_MYSQL_PORT));
    log.warn("MODEL_MYSQL_DB {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_DB));
    modelMysqlConfig.setMysqlModelClientDbName(configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_DB));
    log.warn("MODEL_MYSQL_USER {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_USER));
    modelMysqlConfig.setMysqlModelClientUser(configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_USER));
    log.warn("MODEL_MYSQL_PASS {}", configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_PASS));
    modelMysqlConfig.setMysqlModelClientPassword(configBucket.getString(StaticConfigBucketKeys.MODEL_MYSQL_PASS));
    return modelMysqlConfig;
  }

  private ActiveModelsRetriever getActiveModelsRetriever(DynamicBucket dynamicConfigBucket) {
    return new DynamicBucketActiveModelRetriever(dynamicConfigBucket, DynamicConfigBucketKeys.ACTIVE_MODEL_NAMES_KEY);
  }

  private DynamicBucket getConfigBucket(String bucketName, CryptexClient cryptexClient) {
    ConfigClient client = new ConfigClientBuilder().cryptexClient(cryptexClient).build();
    try {
      return client.getDynamicBucket(bucketName);
    } catch (ConfigServiceException e) {
      throw new RuntimeException("Unable to fetch config bucket", e);
    }
  }
}
