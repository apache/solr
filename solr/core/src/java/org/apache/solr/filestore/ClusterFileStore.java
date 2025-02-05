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

package org.apache.solr.filestore;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.handler.admin.api.ReplicationAPIBase.FILE_STREAM;
import static org.apache.solr.response.RawResponseWriter.CONTENT;

import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ClusterFileStoreApis;
import org.apache.solr.client.api.model.FileStoreDirectoryListingResponse;
import org.apache.solr.client.api.model.FileStoreEntryMetadata;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UploadToFileStoreResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.CryptoKeys;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterFileStore extends JerseyResource implements ClusterFileStoreApis {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FILESTORE_DIRECTORY = "filestore";
  public static final String TRUSTED_DIR = "_trusted_";
  public static final String KEYS_DIR = "/_trusted_/keys";
  static final String TMP_ZK_NODE = "/fileStoreWriteInProgress";

  private final CoreContainer coreContainer;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final FileStore fileStore;

  @Inject
  public ClusterFileStore(
      CoreContainer coreContainer,
      DistribFileStore fileStore,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    this.coreContainer = coreContainer;
    this.req = req;
    this.rsp = rsp;
    this.fileStore = fileStore;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public UploadToFileStoreResponse uploadFile(
      String filePath, List<String> sig, InputStream requestBody) {
    final var response = instantiateJerseyResponse(UploadToFileStoreResponse.class);
    if (!coreContainer.getPackageLoader().getPackageAPI().isEnabled()) {
      throw new RuntimeException(PackageAPI.ERR_MSG);
    }
    try {
      coreContainer
          .getZkController()
          .getZkClient()
          .create(TMP_ZK_NODE, "true".getBytes(UTF_8), CreateMode.EPHEMERAL, true);

      if (requestBody == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no payload");
      if (filePath == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No path");
      }
      validateName(filePath, true);
      try {
        byte[] buf = requestBody.readAllBytes();
        List<String> signatures = readSignatures(sig, buf);
        FileStoreAPI.MetaData meta = _createJsonMetaData(buf, signatures);
        FileStore.FileType type = fileStore.getType(filePath, true);
        if (type == FileStore.FileType.FILE) {
          // a file already exist at the same path
          fileStore.get(
              filePath,
              fileEntry -> {
                if (meta.equals(fileEntry.meta)) {
                  // the file content is same too. this is an idempotent put
                  // do not throw an error
                  response.file = filePath;
                  response.message = "File with same metadata exists ";
                }
              },
              true);
          // 'message' only set in the "already exists w/ same content" case, so we're done!
          if (response.message != null) {
            return response;
          }
        } else if (type != FileStore.FileType.NOFILE) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Path already exists " + filePath);
        }

        fileStore.put(new FileStore.FileEntry(ByteBuffer.wrap(buf), meta, filePath));
        response.file = filePath;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    } catch (InterruptedException e) {
      log.error("Unexpected error", e);
    } catch (KeeperException.NodeExistsException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "A write is already in process , try later");
    } catch (KeeperException e) {
      log.error("Unexpected error", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage());
    } finally {
      try {
        coreContainer.getZkController().getZkClient().delete(TMP_ZK_NODE, -1, true);
      } catch (Exception e) {
        log.error("Unexpected error  ", e);
      }
    }

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_READ_PERM)
  public SolrJerseyResponse getFile(String path) {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);

    final var type = fileStore.getType(path, false);
    if (type == FileStore.FileType.NOFILE) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND,
          "Requested path [" + path + "] not found in filestore");
    } else if (type == FileStore.FileType.DIRECTORY) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Requested path [" + path + "] is a directory and has no returnable contents");
    }

    attachFileToResponse(path, fileStore, req, rsp);
    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_READ_PERM)
  public FileStoreDirectoryListingResponse getMetadata(String path) {
    if (path == null) {
      path = "";
    }
    FileStore.FileType type = fileStore.getType(path, false);
    return getMetadata(type, path, fileStore);
  }

  public static void attachFileToResponse(
      String path, FileStore fileStore, SolrQueryRequest req, SolrQueryResponse rsp) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.WT, FILE_STREAM);
    req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
    rsp.add(
        CONTENT,
        (SolrCore.RawWriter)
            os ->
                fileStore.get(
                    path,
                    it -> {
                      try {
                        InputStream inputStream = it.getInputStream();
                        if (inputStream != null) {
                          inputStream.transferTo(os);
                        }
                      } catch (IOException e) {
                        throw new SolrException(
                            SolrException.ErrorCode.SERVER_ERROR, "Error reading file " + path);
                      }
                    },
                    false));
  }

  @SuppressWarnings("fallthrough")
  public static FileStoreDirectoryListingResponse getMetadata(
      FileStore.FileType type, String path, FileStore fileStore) {
    final var dirListingResponse = new FileStoreDirectoryListingResponse();
    if (path == null) {
      path = "";
    }

    switch (type) {
      case NOFILE:
        dirListingResponse.files = Collections.singletonMap(path, null);
        break;
      case METADATA:
      case FILE:
        int idx = path.lastIndexOf('/');
        String fileName = path.substring(idx + 1);
        String parentPath = path.substring(0, path.lastIndexOf('/'));
        List<FileStore.FileDetails> l = fileStore.list(parentPath, s -> s.equals(fileName));

        dirListingResponse.files =
            Collections.singletonMap(path, l.isEmpty() ? null : convertToResponse(l.get(0)));
        break;
      case DIRECTORY:
        final var directoryContents =
            fileStore.list(path, null).stream()
                .map(details -> convertToResponse(details))
                .collect(Collectors.toList());
        dirListingResponse.files = Collections.singletonMap(path, directoryContents);
        break;
    }

    return dirListingResponse;
  }

  // TODO Modify the filestore implementation itself to return this object, so conversion isn't
  // needed.
  private static FileStoreEntryMetadata convertToResponse(FileStore.FileDetails details) {
    final var entryMetadata = new FileStoreEntryMetadata();

    entryMetadata.name = details.getSimpleName();
    if (details.isDir()) {
      entryMetadata.dir = true;
      return entryMetadata;
    }

    entryMetadata.size = details.size();
    entryMetadata.timestamp = details.getTimeStamp();
    if (details.getMetaData() != null) {
      details.getMetaData().toMap(entryMetadata.unknownProperties());
    }

    return entryMetadata;
  }

  private void doLocalDelete(String filePath) {
    fileStore.deleteLocal(filePath);
  }

  private void doClusterDelete(String filePath) {
    FileStore.FileType type = fileStore.getType(filePath, true);
    if (type == FileStore.FileType.NOFILE) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Path does not exist: " + filePath);
    }

    try {
      coreContainer
          .getZkController()
          .getZkClient()
          .create(TMP_ZK_NODE, "true".getBytes(UTF_8), CreateMode.EPHEMERAL, true);
      fileStore.delete(filePath);
    } catch (Exception e) {
      log.error("Unknown error", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      try {
        coreContainer.getZkController().getZkClient().delete(TMP_ZK_NODE, -1, true);
      } catch (Exception e) {
        log.error("Unexpected error  ", e);
      }
    }
  }

  private void doDelete(String filePath, Boolean localDelete) {
    if (Boolean.TRUE.equals(localDelete)) {
      doLocalDelete(filePath);
    } else {
      doClusterDelete(filePath);
    }
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public SolrJerseyResponse deleteFile(String filePath, Boolean localDelete) {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (!coreContainer.getPackageLoader().getPackageAPI().isEnabled()) {
      throw new RuntimeException(PackageAPI.ERR_MSG);
    }

    validateName(filePath, true);
    if (coreContainer.getPackageLoader().getPackageAPI().isJarInuse(filePath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "jar in use, can't delete");
    }
    doDelete(filePath, localDelete);
    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public SolrJerseyResponse fetchFile(String path, String getFrom) {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (path == null) {
      path = "";
    }
    pullFileFromNode(coreContainer, fileStore, path, getFrom);
    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
  public SolrJerseyResponse syncFile(String path) {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    syncToAllNodes(fileStore, path);
    return response;
  }

  public static void syncToAllNodes(FileStore fileStore, String path) {
    try {
      fileStore.syncToAllNodes(path);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error getting file ", e);
    }
  }

  public static void pullFileFromNode(
      CoreContainer coreContainer, FileStore fileStore, String path, String getFrom) {
    coreContainer
        .getUpdateShardHandler()
        .getUpdateExecutor()
        .submit(
            () -> {
              log.debug("Downloading file {}", path);
              try {
                fileStore.fetch(path, getFrom);
              } catch (Exception e) {
                log.error("Failed to download file: {}", path, e);
              }
              log.info("downloaded file: {}", path);
            });
  }

  private List<String> readSignatures(List<String> signatures, byte[] buf)
      throws SolrException, IOException {
    if (signatures == null || signatures.isEmpty()) return null;
    fileStore.refresh(KEYS_DIR);
    validate(signatures, buf);
    return signatures;
  }

  private void validate(List<String> sigs, byte[] buf) throws SolrException, IOException {
    Map<String, byte[]> keys = fileStore.getKeys();
    if (keys == null || keys.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "File store does not have any keys");
    }
    CryptoKeys cryptoKeys = null;
    try {
      cryptoKeys = new CryptoKeys(keys);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error parsing public keys in file store");
    }
    for (String sig : sigs) {
      if (cryptoKeys.verify(sig, ByteBuffer.wrap(buf)) == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Signature does not match any public key : "
                + sig
                + " len: "
                + buf.length
                + " content sha512: "
                + DigestUtils.sha512Hex(buf));
      }
    }
  }

  /**
   * Creates a JSON string with the metadata.
   *
   * @lucene.internal
   */
  public static FileStoreAPI.MetaData _createJsonMetaData(byte[] buf, List<String> signatures)
      throws IOException {
    String sha512 = DigestUtils.sha512Hex(buf);
    Map<String, Object> vals = new HashMap<>();
    vals.put(FileStoreAPI.MetaData.SHA512, sha512);
    if (signatures != null) {
      vals.put("sig", signatures);
    }
    return new FileStoreAPI.MetaData(vals);
  }

  static final String INVALIDCHARS = " /\\#&*\n\t%@~`=+^$><?{}[]|:;!";

  public static void validateName(String path, boolean failForTrusted) {
    if (path == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty path");
    }
    List<String> parts = StrUtils.splitSmart(path, '/', true);
    for (String part : parts) {
      if (part.charAt(0) == '.') {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "cannot start with period");
      }
      for (int i = 0; i < part.length(); i++) {
        for (int j = 0; j < INVALIDCHARS.length(); j++) {
          if (part.charAt(i) == INVALIDCHARS.charAt(j))
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Unsupported char in file name: " + part);
        }
      }
    }
    if (failForTrusted && TRUSTED_DIR.equals(parts.get(0))) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "trying to write into /_trusted_/ directory");
    }
  }
}
