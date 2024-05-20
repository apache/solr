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
import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.CryptoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FILESTORE_DIRECTORY = "filestore";
  public static final String TRUSTED_DIR = "_trusted_";
  public static final String KEYS_DIR = "/_trusted_/keys";

  private final CoreContainer coreContainer;
  FileStore fileStore;
  public final FSRead readAPI = new FSRead();
  public final FSWrite writeAPI = new FSWrite();

  public FileStoreAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    fileStore = new DistribFileStore(coreContainer);
  }

  public FileStore getFileStore() {
    return fileStore;
  }

  public class FSWrite {

    @EndPoint(
        path = "/node/files/*",
        method = SolrRequest.METHOD.DELETE,
        permission = PermissionNameProvider.Name.FILESTORE_WRITE_PERM)
    public void deleteLocal(SolrQueryRequest req, SolrQueryResponse rsp) {
      String path = req.getPathTemplateValues().get("*");
      validateName(path, true);
      fileStore.deleteLocal(path);
    }
  }

  public class FSRead {
    @EndPoint(
        path = "/node/files/*",
        method = SolrRequest.METHOD.GET,
        permission = PermissionNameProvider.Name.FILESTORE_READ_PERM)
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      String path = req.getPathTemplateValues().get("*");
      String pathCopy = path;
      if (req.getParams().getBool("sync", false)) {
        try {
          fileStore.syncToAllNodes(path);
          return;
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error getting file ", e);
        }
      }
      String getFrom = req.getParams().get("getFrom");
      if (getFrom != null) {
        coreContainer
            .getUpdateShardHandler()
            .getUpdateExecutor()
            .submit(
                () -> {
                  log.debug("Downloading file {}", pathCopy);
                  try {
                    fileStore.fetch(pathCopy, getFrom);
                  } catch (Exception e) {
                    log.error("Failed to download file: {}", pathCopy, e);
                  }
                  log.info("downloaded file: {}", pathCopy);
                });
        return;
      }
      if (path == null) {
        path = "";
      }

      FileStore.FileType typ = fileStore.getType(path, false);
      if (typ == FileStore.FileType.NOFILE) {
        rsp.add("files", Collections.singletonMap(path, null));
        return;
      }
      if (typ == FileStore.FileType.DIRECTORY) {
        rsp.add("files", Collections.singletonMap(path, fileStore.list(path, null)));
        return;
      }
      if (req.getParams().getBool("meta", false)) {
        if (typ == FileStore.FileType.FILE) {
          int idx = path.lastIndexOf('/');
          String fileName = path.substring(idx + 1);
          String parentPath = path.substring(0, path.lastIndexOf('/'));
          List<FileStore.FileDetails> l = fileStore.list(parentPath, s -> s.equals(fileName));
          rsp.add("files", Collections.singletonMap(path, l.isEmpty() ? null : l.get(0)));
          return;
        }
      } else {
        writeRawFile(req, rsp, path);
      }
    }

    private void writeRawFile(SolrQueryRequest req, SolrQueryResponse rsp, String path) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      if ("json".equals(req.getParams().get(CommonParams.WT))) {
        solrParams.add(CommonParams.WT, "json");
        req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
        try {
          fileStore.get(
              path,
              it -> {
                try {
                  InputStream inputStream = it.getInputStream();
                  if (inputStream != null) {
                    rsp.addResponse(new String(inputStream.readAllBytes(), UTF_8));
                  }
                } catch (IOException e) {
                  throw new SolrException(
                      SolrException.ErrorCode.SERVER_ERROR, "Error reading file " + path);
                }
              },
              false);
        } catch (IOException e) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR, "Error getting file from path " + path);
        }
      } else {
        solrParams.add(CommonParams.WT, FILE_STREAM);
        req.setParams(SolrParams.wrapDefaults(solrParams, req.getParams()));
        rsp.add(
            FILE_STREAM,
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
    }
  }

  public static class MetaData implements MapWriter {
    public static final String SHA512 = "sha512";
    String sha512;
    List<String> signatures;
    Map<String, Object> otherAttribs;

    @SuppressWarnings("unchecked")
    public MetaData(Map<String, Object> m) {
      m = (Map<String, Object>) Utils.getDeepCopy(m, 3);
      this.sha512 = (String) m.remove(SHA512);
      this.signatures = (List<String>) m.remove("sig");
      this.otherAttribs = m;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.putIfNotNull("sha512", sha512);
      ew.putIfNotNull("sig", signatures);
      if (!otherAttribs.isEmpty()) {
        otherAttribs.forEach(ew.getBiConsumer());
      }
    }

    @Override
    public int hashCode() {
      return sha512.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof MetaData) {
        MetaData metaData = (MetaData) that;
        return Objects.equals(sha512, metaData.sha512)
            && Objects.equals(signatures, metaData.signatures)
            && Objects.equals(otherAttribs, metaData.otherAttribs);
      }
      return false;
    }
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

  /**
   * Validate a file for signature
   *
   * @param sigs the signatures. atleast one should succeed
   * @param entry The file details
   * @param isFirstAttempt If there is a failure
   */
  public void validate(List<String> sigs, FileStore.FileEntry entry, boolean isFirstAttempt)
      throws SolrException, IOException {
    if (!isFirstAttempt) {
      // we are retrying because last validation failed.
      // get all keys again and try again
      fileStore.refresh(KEYS_DIR);
    }

    Map<String, byte[]> keys = fileStore.getKeys();
    if (keys == null || keys.isEmpty()) {
      if (isFirstAttempt) {
        validate(sigs, entry, false);
        return;
      }
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Filestore does not have any public keys");
    }
    CryptoKeys cryptoKeys = null;
    try {
      cryptoKeys = new CryptoKeys(keys);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error parsing public keys in ZooKeeper");
    }
    for (String sig : sigs) {
      Supplier<String> errMsg =
          () ->
              "Signature does not match any public key : "
                  + sig
                  + "sha256 "
                  + entry.getMetaData().sha512;
      if (entry.getBuffer() != null) {
        if (cryptoKeys.verify(sig, entry.getBuffer()) == null) {
          if (isFirstAttempt) {
            validate(sigs, entry, false);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }
      } else {
        InputStream inputStream = entry.getInputStream();
        if (cryptoKeys.verify(sig, inputStream) == null) {
          if (isFirstAttempt) {
            validate(sigs, entry, false);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }
      }
    }
  }
}
