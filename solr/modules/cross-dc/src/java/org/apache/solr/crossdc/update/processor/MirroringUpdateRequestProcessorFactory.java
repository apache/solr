/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.update.processor;

import static org.apache.solr.crossdc.common.CrossDcConf.EXPAND_DBQ;
import static org.apache.solr.crossdc.common.CrossDcConf.ExpandDbq;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.MAX_REQUEST_SIZE_BYTES;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.MIRROR_COMMITS;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.COMMIT_END_POINT;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM_COLLECTION;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM_PARENT;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM_SHARD;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.ConfUtil;
import org.apache.solr.crossdc.common.ConfigProperty;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DocBasedVersionConstraintsProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An update processor that works with the {@link UpdateRequestProcessorFactory} to mirror update
 * requests by submitting them to a sink that implements a queue producer.
 *
 * <p>ADDs and DeleteByIDs are mirrored from leader shards and have internal _version_ fields
 * stripped. node.
 *
 * <p>A single init arg is required, <b>requestMirroringHandler</b>, which specifies the plugin
 * class used for mirroring requests. This class must implement {@link RequestMirroringHandler}.
 *
 * <p>It is recommended to use the {@link DocBasedVersionConstraintsProcessorFactory} upstream of
 * this factory to ensure doc consistency between this cluster and the mirror(s).
 */
public class MirroringUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // Flag for mirroring requests
  public static final String SERVER_SHOULD_MIRROR = "shouldMirror";

  /**
   * This is instantiated in inform(SolrCore) and then shared by all processor instances - visible
   * for testing
   */
  private volatile KafkaRequestMirroringHandler mirroringHandler;

  private volatile ProducerMetrics producerMetrics;

  private boolean enabled = true;

  private KafkaCrossDcConf conf;

  private final Map<String, Object> properties = new HashMap<>();

  private NamedList<?> args;

  @Override
  public void init(final NamedList<?> args) {
    super.init(args);

    this.args = args;
  }

  private void applyArgsOverrides() {
    Boolean enabled = args.getBooleanArg("enabled");
    if (enabled != null && !enabled) {
      this.enabled = false;
    }
    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
      String val = args._getStr(configKey.getKey(), null);
      if (val != null && !val.isBlank()) {
        properties.put(configKey.getKey(), val);
      }
    }
  }

  private static class MyCloseHook implements CloseHook {
    private final Closer closer;

    public MyCloseHook(Closer closer) {
      this.closer = closer;
    }

    @Override
    public void preClose(SolrCore core) {}

    @Override
    public void postClose(SolrCore core) {
      closer.close();
    }
  }

  private static class Closer {
    private final KafkaMirroringSink sink;

    public Closer(KafkaMirroringSink sink) {
      this.sink = sink;
    }

    public final void close() {
      try {
        this.sink.close();
      } catch (IOException e) {
        log.error("Exception closing sink", e);
      }
    }
  }

  private void lookupPropertyOverridesInZk(SolrCore core) {
    log.info(
        "Producer startup config properties before adding additional properties from Zookeeper={}",
        properties);

    try {
      SolrZkClient solrZkClient = core.getCoreContainer().getZkController().getZkClient();
      ConfUtil.fillProperties(solrZkClient, properties);
      applyArgsOverrides();
      CollectionProperties cp = new CollectionProperties(solrZkClient);
      Map<String, String> collectionProperties =
          cp.getCollectionProperties(core.getCoreDescriptor().getCollectionName());
      for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
        String val = collectionProperties.get("crossdc." + configKey.getKey());
        if (val != null && !val.isBlank()) {
          properties.put(configKey.getKey(), val);
        }
      }
      String enabledVal = collectionProperties.get("crossdc.enabled");
      if (enabledVal != null) {
        if (Boolean.parseBoolean(enabledVal.toString())) {
          this.enabled = true;
        } else {
          this.enabled = false;
        }
      }
    } catch (Exception e) {
      log.error("Exception looking for CrossDC configuration in Zookeeper", e);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Exception looking for CrossDC configuration in Zookeeper",
          e);
    }
  }

  @Override
  public void inform(SolrCore core) {
    log.info("KafkaRequestMirroringHandler inform enabled={}", this.enabled);

    if (core.getCoreContainer().isZooKeeperAware()) {
      lookupPropertyOverridesInZk(core);
    } else {
      applyArgsOverrides();
      if (enabled) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            getClass().getSimpleName()
                + " only supported in SolrCloud mode; please disable or remove from solrconfig.xml");
      }
      log.warn(
          "Core '{}' was configured to use a disabled {}, but {} is only supported in SolrCloud deployments.  A NoOp processor will be used instead",
          core.getName(),
          this.getClass().getSimpleName(),
          this.getClass().getSimpleName());
    }

    if (!enabled) {
      return;
    }

    ConfUtil.verifyProperties(properties);

    // load the request mirroring sink class and instantiate.
    // mirroringHandler =
    // core.getResourceLoader().newInstance(RequestMirroringHandler.class.getName(),
    // KafkaRequestMirroringHandler.class);

    conf = new KafkaCrossDcConf(properties);

    KafkaMirroringSink sink = new KafkaMirroringSink(conf);

    Closer closer = new Closer(sink);
    core.addCloseHook(new MyCloseHook(closer));

    producerMetrics = new ProducerMetrics(core.getSolrMetricsContext().getChildContext(this), core);
    mirroringHandler = new KafkaRequestMirroringHandler(sink);
  }

  private static Integer getIntegerPropValue(String name, Properties props) {
    String value = props.getProperty(name);
    if (value == null) {
      return null;
    }
    return Integer.parseInt(value);
  }

  @Override
  public UpdateRequestProcessor getInstance(
      final SolrQueryRequest req, final SolrQueryResponse rsp, final UpdateRequestProcessor next) {

    if (!enabled) {
      return new NoOpUpdateRequestProcessor(next);
    }

    // if the class fails to initialize
    if (mirroringHandler == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "mirroringHandler is null");
    }

    // Check if mirroring is disabled in request params, defaults to true
    boolean doMirroring = req.getParams().getBool(SERVER_SHOULD_MIRROR, true);
    boolean mirrorCommits = conf.getBool(MIRROR_COMMITS);
    ExpandDbq expandDbq = ExpandDbq.getOrDefault(conf.get(EXPAND_DBQ), ExpandDbq.EXPAND);
    final long maxMirroringBatchSizeBytes = conf.getInt(MAX_REQUEST_SIZE_BYTES);
    Boolean indexUnmirrorableDocs = conf.getBool(INDEX_UNMIRRORABLE_DOCS);

    ModifiableSolrParams mirroredParams = null;
    if (doMirroring) {
      // Get the collection name for the core so we can be explicit in the mirrored request
      CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
      String collection = coreDesc.getCollectionName();
      if (collection == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not determine collection name for "
                + MirroringUpdateProcessor.class.getSimpleName()
                + ". Solr may not be running in cloud mode.");
      }

      mirroredParams = new ModifiableSolrParams(req.getParams());
      mirroredParams.set("collection", collection);
      // remove internal version parameter
      mirroredParams.remove(CommonParams.VERSION_FIELD);
      // remove fields added by distributed update proc
      mirroredParams.remove(DISTRIB_UPDATE_PARAM);
      mirroredParams.remove(DISTRIB_FROM_COLLECTION);
      mirroredParams.remove(DISTRIB_INPLACE_PREVVERSION);
      mirroredParams.remove(COMMIT_END_POINT);
      mirroredParams.remove(DISTRIB_FROM_SHARD);
      mirroredParams.remove(DISTRIB_FROM_PARENT);
      mirroredParams.remove(DISTRIB_FROM);
      // prevent circular mirroring
      mirroredParams.set(SERVER_SHOULD_MIRROR, Boolean.FALSE.toString());
    }
    if (log.isTraceEnabled()) {
      log.trace("Create MirroringUpdateProcessor with mirroredParams={}", mirroredParams);
    }

    return new MirroringUpdateProcessor(
        next,
        doMirroring,
        indexUnmirrorableDocs,
        mirrorCommits,
        expandDbq,
        maxMirroringBatchSizeBytes,
        mirroredParams,
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM)),
        doMirroring ? mirroringHandler : null,
        producerMetrics);
  }

  public static class NoOpUpdateRequestProcessor extends UpdateRequestProcessor {
    NoOpUpdateRequestProcessor(UpdateRequestProcessor next) {
      super(next);
    }
  }
}
