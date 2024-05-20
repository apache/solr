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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.CryptoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common utilities used by filestore-related code. */
public class FileStoreUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** get a list of nodes randomly shuffled * @lucene.internal */
  public static ArrayList<String> shuffledNodes(CoreContainer coreContainer) {
    Set<String> liveNodes =
        coreContainer.getZkController().getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    l.remove(coreContainer.getZkController().getNodeName());
    Collections.shuffle(l, BlobRepository.RANDOM);
    return l;
  }

  public static void validateFiles(
      FileStore fileStore, List<String> files, boolean validateSignatures, Consumer<String> errs) {
    for (String path : files) {
      try {
        FileStore.FileType type = fileStore.getType(path, true);
        if (type != FileStore.FileType.FILE) {
          errs.accept("No such file: " + path);
          continue;
        }

        fileStore.get(
            path,
            entry -> {
              if (entry.getMetaData().signatures == null
                  || entry.getMetaData().signatures.isEmpty()) {
                errs.accept(path + " has no signature");
                return;
              }
              if (validateSignatures) {
                try {
                  fileStore.refresh(ClusterFileStore.KEYS_DIR);
                  validate(fileStore, entry.meta.signatures, entry, false);
                } catch (Exception e) {
                  log.error("Error validating package artifact", e);
                  errs.accept(e.getMessage());
                }
              }
            },
            false);
      } catch (Exception e) {
        log.error("Error reading file ", e);
        errs.accept("Error reading file " + path + " " + e.getMessage());
      }
    }
  }

  /**
   * Validate a file for signature
   *
   * @param sigs the signatures. atleast one should succeed
   * @param entry The file details
   * @param isFirstAttempt If there is a failure
   */
  public static void validate(
      FileStore fileStore, List<String> sigs, FileStore.FileEntry entry, boolean isFirstAttempt)
      throws SolrException, IOException {
    if (!isFirstAttempt) {
      // we are retrying because last validation failed.
      // get all keys again and try again
      fileStore.refresh(ClusterFileStore.KEYS_DIR);
    }

    Map<String, byte[]> keys = fileStore.getKeys();
    if (keys == null || keys.isEmpty()) {
      if (isFirstAttempt) {
        validate(fileStore, sigs, entry, false);
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
            validate(fileStore, sigs, entry, false);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }
      } else {
        InputStream inputStream = entry.getInputStream();
        if (cryptoKeys.verify(sig, inputStream) == null) {
          if (isFirstAttempt) {
            validate(fileStore, sigs, entry, false);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errMsg.get());
        }
      }
    }
  }
}
