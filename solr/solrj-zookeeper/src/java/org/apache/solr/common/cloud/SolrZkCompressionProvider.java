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
package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import org.apache.curator.framework.api.CompressionProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.ZLibCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrZkCompressionProvider implements CompressionProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Compressor compressor;
  private final int minStateByteLenForCompression;

  public SolrZkCompressionProvider(Compressor compressor, int minStateByteLenForCompression) {
    this.compressor = compressor != null ? compressor : new ZLibCompressor();
    this.minStateByteLenForCompression = minStateByteLenForCompression;
  }

  @Override
  public byte[] compress(String path, byte[] data) throws Exception {
    if (path.endsWith("state.json")
        && minStateByteLenForCompression > -1
        && data.length >= minStateByteLenForCompression) {
      // state.json should be compressed before being put to ZK
      return compressor.compressBytes(data);
    } else {
      return data;
    }
  }

  @Override
  public byte[] decompress(String path, byte[] compressedData) throws SolrException {
    if (compressor.isCompressedBytes(compressedData)) {
      log.debug("Zookeeper data at path {} is compressed", path);
      try {
        return compressor.decompressBytes(compressedData);
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            String.format(
                Locale.ROOT, "Unable to decompress data at path: %s from zookeeper", path),
            e);
      }
    } else {
      return compressedData;
    }
  }
}
