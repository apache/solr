package org.apache.solr.handler.admin.api;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpgradeCoreIndexRequestBody;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DateValueFieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocValuesIteratorCache;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeCoreIndex extends CoreAdminAPIBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * The re-indexing status at any point of time for a particular core.
   * DEFAULT - This is the default status, meaning it is yet to be processed and checked if the version is LATEST
   * for this core
   * REINDEXING_ACTIVE - This is set at the start of the re-indexing operation
   * PROCESSED - This is set at the end of the re-indexing operation if there are no errors
   * ERROR - This is set if there is any error in any segment. This core will be retried CORE_ERROR_RETRIES number
   * of
   * times
   * CORRECTVERSION - This is set if the core is already at the correct version
   */
  public enum CoreReindexingStatus {
    DEFAULT,
    REINDEXING_ACTIVE,
    REINDEXING_PAUSED,
    PROCESSED,
    ERROR,
    CORRECTVERSION;
  }

  /*
   * The state that a single ReindexingThread would be in. This is set to START_REINDEXING by CPUMonitorTask
   * thread
   * START_REINDEXING - CPUMonitorTask checks if current CPU usage is below given threshold and sets this state
   * WAITING - CPUMonitorTask checks if the CPU usage is above given threshold and sets this state to put
   * the CVReindexingTask thread in a waiting state. Note that ReindexingTask thread run() will still be checked
   * in this case. This can also be set when all cores have processed and there are no pending cores.
   * STOP_REINDEXING - CPUMonitorTask checks if all cores are processed with the status between {CORRECTVERSION,
   * ERROR}
   * and if all cores are processed then sets it to STOP_REINDEXING
   *
   */
  public enum ReindexingThreadState {
    START_REINDEXING,
    STOP_REINDEXING,
    WAITING;
  }

  private static final int SEGMENT_ERROR_RETRIES = 3;
  private static final long SLEEP_TIME_BEFORE_AFTER_COMMIT_MS = 10000;
  private static final int RETRY_COUNT_FOR_SEGMENT_DELETION = 5;
  private static final long SLEEP_TIME_SEGMENT_DELETION_MS = 60000;

  private static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

  public UpgradeCoreIndex(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  public boolean isExpensive() {
    return true;
  }

  public SolrJerseyResponse upgradeCoreIndex(
      String coreName, UpgradeCoreIndexRequestBody requestBody) throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        requestBody.async,
        "upgrade-index",
        () -> {
          try (SolrCore core = coreContainer.getCore(coreName)) {

            log.info("Received UPGRADECOREINDEX request for core: {}", core.getName());
            CoreReindexingStatus coreRxStatus = CoreReindexingStatus.REINDEXING_ACTIVE;
            RefCounted<SolrIndexSearcher> ssearcherRef = core.getSearcher();
            List<LeafReaderContext> leafContexts =
                ssearcherRef.get().getTopReaderContext().leaves();
            DocValuesIteratorCache dvICache = new DocValuesIteratorCache(ssearcherRef.get());

            UpdateRequestProcessorChain updateProcessorChain =
                getUpdateProcessorChain(core, requestBody.updateChain);

            try {

              for (LeafReaderContext lrc : leafContexts) {
                if (!shouldUpgradeSegment(lrc)) {
                  continue;
                }

                boolean segmentError = true;

                for (int i = 0; i < SEGMENT_ERROR_RETRIES; i++) {
                  // retrying segment; I anticipate throttling to be the main reason in most
                  // cases
                  // hence the sleep
                  if (i > 0) {
                    Thread.sleep(5 * 60 * 1000); // 5 minutes
                  }

                  segmentError = processSegment(lrc, updateProcessorChain, core, dvICache);

                  if (!segmentError) {
                    break;
                  }
                }
                if (segmentError) {
                  coreRxStatus = CoreReindexingStatus.ERROR;
                }
              }
            } catch (Exception e) {
              log.error("Error while processing core: {}, exception: {}", coreName, e.toString());
              coreRxStatus = CoreReindexingStatus.ERROR;
            }

            try {
              RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(null);
              if (iwRef != null) {
                IndexWriter iw = iwRef.get();
                try {
                  if (iw != null) {
                    iw.commit();
                  } else {
                    log.warn("IndexWriter for core {} is null", core.getName());
                  }
                } finally {
                  iwRef.decref();
                }
              } else {
                log.warn("IWRef for core {} is null", core.getName());
              }
            } catch (IOException ioEx) {

            }

            // important to decrement searcher ref count after use since we obtained it via the
            // SolrCore.getSearcher() method
            ssearcherRef.decref();

            // IF coreRxStatus == CoreReindexingStatus.REINDEXING_PAUSED at this point then most
            // likely it
            // reached here
            // by breaking out of segment processing. So we are going straight to setting the state
            // and
            // publishing to reindexing_status.csv
            if (coreRxStatus != CoreReindexingStatus.REINDEXING_PAUSED) {
              try {
                if (coreRxStatus == CoreReindexingStatus.ERROR) {
                  log.error("Core CoreReindexingStatus returned error, not calling commit");
                } else {
                  Boolean validationResult = false;
                  for (int i = 0;
                      (i < RETRY_COUNT_FOR_SEGMENT_DELETION)
                          && (validationResult != null && !validationResult);
                      i++) {

                    doCommit(core);
                    Thread.sleep(SLEEP_TIME_BEFORE_AFTER_COMMIT_MS);

                    validationResult = validateSegmentsUpdated(core);
                    log.warn(
                        "validateSegmentsUpdated() returned: {} for core: {}, sleeping for {}ms before calling commit...",
                        validationResult,
                        coreName,
                        SLEEP_TIME_SEGMENT_DELETION_MS);
                    Thread.sleep(SLEEP_TIME_SEGMENT_DELETION_MS);
                  }
                  if ((validationResult == null)
                      || (validationResult != null && !validationResult)) {
                    log.error(
                        "Validation failed for core: {}, not increasing indexCreatedVersionMajor",
                        validationResult,
                        coreName);
                    coreRxStatus = CoreReindexingStatus.ERROR;
                  } else {

                    doCommit(core);
                    Thread.sleep(SLEEP_TIME_BEFORE_AFTER_COMMIT_MS);

                    int indexCreatedVersionMajorAfterCommit = getIndexCreatedVersionMajor(core);
                    log.info(
                        "Post processing coreName: {}, indexCreatedVersionMajorAfterCommit: {}",
                        coreName,
                        indexCreatedVersionMajorAfterCommit);
                    if (indexCreatedVersionMajorAfterCommit == Version.LATEST.major) {
                      log.info(
                          "Core: {} index version updated successfully to {}",
                          coreName,
                          Version.LATEST);
                      coreRxStatus = CoreReindexingStatus.PROCESSED;
                    } else {
                      log.error(
                          "indexCreatedVersionMajorAfterCommit is {}",
                          indexCreatedVersionMajorAfterCommit);
                      coreRxStatus = CoreReindexingStatus.ERROR;
                    }
                  }
                }
              } catch (Exception e) {
                log.error("Exception in processCore: {}", e.toString());
              }
            }
          }
          return null;
        });
  }

  private boolean shouldUpgradeSegment(LeafReaderContext lrc) {
    Version segmentMinVersion = null;
    try (LeafReader leafReader = lrc.reader()) {
      segmentMinVersion = leafReader.getMetaData().minVersion();
    } catch (IOException ex) {
      // TO-DO
      // Wrap exception in CoreAdminAPIBaseException
    }
    return (segmentMinVersion == null || segmentMinVersion.major < Version.LATEST.major);
  }

  private int getIndexCreatedVersionMajor(SolrCore core) {
    int indexCreatedVersionMajor = 0;
    try (FSDirectory dir = FSDirectory.open(Paths.get(core.getIndexDir()))) {
      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      indexCreatedVersionMajor = sis.getIndexCreatedVersionMajor();
    } catch (Exception e) {
      log.error(
          "Error while opening segmentInfos for core: {}, exception: {}",
          core.getName(),
          e.toString());
    }

    return indexCreatedVersionMajor;
  }

  @SuppressWarnings({"rawtypes"})
  private UpdateRequestProcessorChain getUpdateProcessorChain(
      SolrCore core, String requestedUpdateChain) {

    UpdateRequestProcessorChain resolvedChain = null;
    if (requestedUpdateChain != null) {
      resolvedChain = core.getUpdateProcessingChain(requestedUpdateChain);
      if (resolvedChain != null) {
        return resolvedChain;
      }
      log.warn(
          "Requested update chain {} not found for core {}, falling back to default",
          requestedUpdateChain,
          core.getName());
    }

    SolrRequestHandler reqHandler = core.getRequestHandler("/update");
    NamedList initArgs = ((RequestHandlerBase) reqHandler).getInitArgs();

    String updateChainName = null;
    Object defaults = initArgs.get("defaults");
    if (defaults != null && defaults instanceof NamedList) {
      updateChainName = (String) (((NamedList) defaults).get(UpdateParams.UPDATE_CHAIN));
    }
    if (updateChainName == null) {
      Object invariants = initArgs.get("invariants");
      if (invariants != null && invariants instanceof NamedList) {
        updateChainName = (String) (((NamedList) invariants).get(UpdateParams.UPDATE_CHAIN));
      }
    }

    resolvedChain = core.getUpdateProcessingChain(updateChainName);
    if (resolvedChain == null) {
      resolvedChain = core.getUpdateProcessingChain(null);
    }
    return resolvedChain;
  }

  /*
   * returns:
   *
   * null: For any error or if there is at least one older version segment present in the index
   * false: For any 0 older version segment present in the index having 0 numDocs
   * true: If all segments are LATEST version
   *
   */
  private Boolean validateSegmentsUpdated(SolrCore core) {
    Boolean segmentsUpdated = null;
    try (FSDirectory dir = FSDirectory.open(Paths.get(core.getIndexDir()));
        IndexReader reader = DirectoryReader.open(dir)) {

      List<LeafReaderContext> leaves = reader.leaves();
      if (leaves == null || leaves.isEmpty()) {
        // no segments to process/validate
        return true;
      }
      segmentsUpdated = true;
      for (LeafReaderContext lrc : leaves) {
        LeafReader leafReader = lrc.reader();
        leafReader = FilterLeafReader.unwrap(leafReader);
        if (leafReader instanceof SegmentReader) {
          SegmentReader segmentReader = (SegmentReader) leafReader;
          SegmentCommitInfo si = segmentReader.getSegmentInfo();
          Version segMinVersion = si.info.getMinVersion();
          if (segMinVersion == null || segMinVersion.major != Version.LATEST.major) {
            log.warn(
                "validateSegmentsUpdated(): Core: {}, Segment {} is still at minVersion: {} and is not updated to the latest version {}",
                core.getName(),
                si.info.name,
                (segMinVersion == null ? 6 : segMinVersion.major),
                Version.LATEST.major);
            segmentsUpdated = null;
            // Since we could have 1 0-numDoc segment and multiple non-zero numDoc
            // older version segments, we break only if a 0-numDoc segment is found
            if (segmentReader.numDocs() == 0) {
              segmentsUpdated = false;
              break;
            }
          }
        }
      }
    } catch (Exception e) {
      log.error(
          "Error while opening segmentInfos for core: {}, exception: {}",
          core.getName(),
          e.toString());
      segmentsUpdated = null;
    }
    return segmentsUpdated;
  }

  private void doCommit(SolrCore core) {
    try {
      UpdateRequest updateReq = new UpdateRequest();

      ModifiableSolrParams msp = new ModifiableSolrParams();

      msp.add("commit", "true");
      LocalSolrQueryRequest solrReq;
      solrReq = getLocalUpdateReq(updateReq, core, msp);
      doLocalUpdateReq(solrReq, core);

    } catch (Exception e) {
      log.error(
          "Error while sending update request to advance index created version {}", e.toString());
    }
  }

  private void deleteDummyDocAndCommit(SolrCore core, String dummyContentId) throws Exception {
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.deleteById(dummyContentId);
    log.debug("Deleting dummy doc with id: {}", dummyContentId);
    ModifiableSolrParams msp = new ModifiableSolrParams();

    msp.add("commit", "true");
    LocalSolrQueryRequest solrReq;
    try {
      solrReq = getLocalUpdateReq(updateReq, core, msp);
      doLocalUpdateReq(solrReq, core);
    } catch (Exception e) {
      log.error("Error deleting dummy doc");
      throw e;
    }
  }

  public LocalSolrQueryRequest getLocalUpdateReq(
      UpdateRequest updateReq, SolrCore core, ModifiableSolrParams msp) throws IOException {
    LocalSolrQueryRequest solrReq = new LocalSolrQueryRequest(core, msp);
    solrReq.setContentStreams(updateReq.getContentStreams());
    return solrReq;
  }

  public static void doLocalUpdateReq(LocalSolrQueryRequest solrReq, SolrCore core) {
    try {
      SolrQueryResponse resp = new SolrQueryResponse();
      core.getRequestHandler("/update").handleRequest(solrReq, resp);
      if (resp.getException() != null) {
        log.error("doLocalUpdateReq error: {}", resp.getException().toString());
      }
    } catch (Exception e) {
      log.error("Exception in doLocalUpdateReq: {}", e.toString());
    } finally {
      solrReq.close();
    }
  }

  private SolrInputDocument getDummyDoc(SolrCore core) {
    SolrInputDocument dummyDoc = new SolrInputDocument();
    String dummyContentId = "cvrx-dummydoc" + UUID.randomUUID().toString();
    String uniqeKeyFieldName = core.getLatestSchema().getUniqueKeyField().getName();
    dummyDoc.addField(uniqeKeyFieldName, dummyContentId);
    Collection<SchemaField> requiredFields = core.getLatestSchema().getRequiredFields();

    for (SchemaField sf : requiredFields) {
      if (sf.getName().equals(uniqeKeyFieldName) || sf.getDefaultValue() != null) {
        continue;
      }
      if (sf.getType() instanceof DateValueFieldType) {
        dummyDoc.addField(sf.getName(), new Date());
      } else {
        dummyDoc.addField(sf.getName(), "1");
      }
    }
    return dummyDoc;
  }

  private boolean processSegment(
      LeafReaderContext leafReaderContext,
      UpdateRequestProcessorChain processorChain,
      SolrCore core,
      DocValuesIteratorCache dvICache) {

    boolean segmentError = false;
    int numDocsProcessed = 0;

    String coreName = core.getName();
    IndexSchema indexSchema = core.getLatestSchema();

    LeafReader leafReader = FilterLeafReader.unwrap(leafReaderContext.reader());
    SegmentReader segmentReader = (SegmentReader) leafReader;
    Bits bits = segmentReader.getLiveDocs();
    SolrInputDocument solrDoc = null;
    UpdateRequestProcessor processor = null;
    LocalSolrQueryRequest solrRequest = null;
    RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
    SolrDocumentFetcher docFetcher = searcherRef.get().getDocFetcher();
    try {
      // Exclude copy field targets to avoid duplicating values on reindex
      Set<String> fields = docFetcher.getNonStoredDVsWithoutCopyTargets();
      solrRequest = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

      SolrQueryResponse rsp = new SolrQueryResponse();
      processor = processorChain.createProcessor(solrRequest, rsp);
      StoredFields storedFields = segmentReader.storedFields();
      for (int luceneDocId = 0; luceneDocId < segmentReader.maxDoc(); luceneDocId++) {
        if (bits != null && !bits.get(luceneDocId)) {
          continue;
        }

        Document doc = storedFields.document(luceneDocId);
        solrDoc = toSolrInputDocument(doc, indexSchema);

        docFetcher.decorateDocValueFields(
            solrDoc, leafReaderContext.docBase + luceneDocId, fields, dvICache);
        solrDoc.removeField("_version_");
        AddUpdateCommand currDocCmd = new AddUpdateCommand(solrRequest);
        currDocCmd.solrDoc = solrDoc;
        processor.processAdd(currDocCmd);
        numDocsProcessed++;
      }
    } catch (Exception e) {
      log.error("Error in CvReindexingTask process() : {}", e.toString());
      segmentError = true;
    } finally {
      if (processor != null) {
        try {
          processor.finish();
        } catch (Exception e) {
          log.error("Exception while doing finish processor.finish() : {}", e.toString());
          segmentError = true;
        } finally {
          try {
            processor.close();
          } catch (IOException e) {
            log.error("Exception while closing processor: {}", e.toString());
            segmentError = true;
          }
        }
      }
      if (solrRequest != null) {
        solrRequest.close();
      }
      searcherRef.decref();
    }

    log.info(
        "End processing segment : {}, core: {} docs processed: {}",
        segmentReader.getSegmentName(),
        coreName,
        numDocsProcessed);

    return segmentError;
  }

  /*
   * Convert a lucene Document to a SolrInputDocument
   */
  protected SolrInputDocument toSolrInputDocument(
      org.apache.lucene.document.Document doc, IndexSchema schema) {
    SolrInputDocument out = new SolrInputDocument();
    for (IndexableField f : doc.getFields()) {
      String fname = f.name();
      SchemaField sf = schema.getFieldOrNull(f.name());
      Object val = null;
      if (sf != null) {
        if ((!sf.hasDocValues() && !sf.stored()) || schema.isCopyFieldTarget(sf)) {
          continue;
        }
        val = sf.getType().toObject(f);
      } else {
        val = f.stringValue();
        if (val == null) {
          val = f.numericValue();
        }
        if (val == null) {
          val = f.binaryValue();
        }
        if (val == null) {
          val = f;
        }
      }
      out.addField(fname, val);
    }
    return out;
  }
}
