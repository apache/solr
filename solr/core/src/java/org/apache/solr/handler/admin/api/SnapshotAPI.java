package org.apache.solr.handler.admin.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.Collection;
import java.util.Optional;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

@Path("/cores/{coreName}/snapshots")
public class SnapshotAPI extends AdminAPIBase {

    @Inject
    public SnapshotAPI(
            CoreContainer coreContainer,
            SolrQueryRequest solrQueryRequest,
            SolrQueryResponse solrQueryResponse) {
        super(coreContainer, solrQueryRequest, solrQueryResponse);
    }

    @POST
    @Path("/{snapshotName}")
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(CORE_EDIT_PERM)
    public CreateSnapshotResponse createSnapshot(
            @PathParam("coreName") String coreName,
            @PathParam("snapshotName") String snapshotName) throws Exception {
        final CreateSnapshotResponse response = new CreateSnapshotResponse();

        try (SolrCore core = coreContainer.getCore(coreName)) {
            if (core == null) {
                throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
            }

            final String indexDirPath = core.getIndexDir();
            final IndexDeletionPolicyWrapper delPol = core.getDeletionPolicy();
            final IndexCommit ic = delPol.getAndSaveLatestCommit();
            try {
                if (null == ic) {
                    throw new SolrException(
                            SolrException.ErrorCode.BAD_REQUEST, "No index commits to snapshot in core " + coreName);
                }
                final SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
                mgr.snapshot(snapshotName, indexDirPath, ic.getGeneration());

                response.core = core.getName();
                response.commitName = snapshotName;
                response.indexDirPath = indexDirPath;
                response.generation = ic.getGeneration();
                response.files = ic.getFileNames();
            } finally {
                delPol.releaseCommitPoint(ic);
            }
        }

        return response;
    }

    public static class CreateSnapshotResponse extends SolrJerseyResponse {
        @JsonProperty(CoreAdminParams.CORE)
        public String core;

        @JsonProperty(CoreAdminParams.COMMIT_NAME)
        public String commitName;

        @JsonProperty(SolrSnapshotManager.INDEX_DIR_PATH)
        public String indexDirPath;

        @JsonProperty(SolrSnapshotManager.GENERATION_NUM)
        public long generation;

        @JsonProperty(SolrSnapshotManager.FILE_LIST)
        public Collection<String> files;
    }

    @GET
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(CORE_READ_PERM)
    @SuppressWarnings({"unchecked"})
    public ListSnapshotsResponse listSnapshots(
            @PathParam("coreName") String coreName) {
        final ListSnapshotsResponse response = new ListSnapshotsResponse();

        try (SolrCore core = coreContainer.getCore(coreName)) {
            if (core == null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
            }

            SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
            @SuppressWarnings({"rawtypes"})
            NamedList<Object> result = new SimpleOrderedMap<>();
            for (String name : mgr.listSnapshots()) {
                Optional<SolrSnapshotMetaDataManager.SnapshotMetaData> metadata = mgr.getSnapshotMetaData(name);
                if (metadata.isPresent()) {
                    NamedList<String> props = new NamedList<>();
                    props.add(
                            SolrSnapshotManager.GENERATION_NUM,
                            String.valueOf(metadata.get().getGenerationNumber()));
                    props.add(SolrSnapshotManager.INDEX_DIR_PATH, metadata.get().getIndexDirPath());
                    result.add(name, props);
                }
            }

            response.snapshots = result;
        }

        return response;
    }

    public static class ListSnapshotsResponse extends SolrJerseyResponse {
        @JsonProperty(SolrSnapshotManager.SNAPSHOTS_INFO)
        @SuppressWarnings({"rawtypes"})
        public NamedList<Object> snapshots;
    }

    @DELETE
    @Path("/{snapshotName}")
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(CORE_EDIT_PERM)
    public DeleteSnapshotResponse deleteSnapshot(
            @PathParam("coreName") String coreName,
            @PathParam("snapshotName") String snapshotName) throws Exception {
        final DeleteSnapshotResponse response = new DeleteSnapshotResponse();

        final SolrCore core = coreContainer.getCore(coreName);
        if (core == null) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
        }

        try {
            core.deleteNamedSnapshot(snapshotName);
            // Ideally we shouldn't need this. This is added since the RPC logic in
            // OverseerCollectionMessageHandler can not provide the coreName as part of the result.
            response.coreName = coreName;
            response.commitName = snapshotName;
        } finally {
            core.close();
        }

        return response;
    }

    public static class DeleteSnapshotResponse extends SolrJerseyResponse {
        @JsonProperty(CoreAdminParams.CORE)
        public String coreName;

        @JsonProperty(CoreAdminParams.COMMIT_NAME)
        public String commitName;
    }
}
