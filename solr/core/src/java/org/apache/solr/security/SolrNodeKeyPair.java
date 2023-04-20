/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import java.io.IOException;
import java.net.URL;
import java.security.spec.InvalidKeySpecException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.util.CryptoKeys;

/**
 * Creates and mediates access to the CryptoKeys.RSAKeyPair used by this Solr node.
 *
 * <p>Expected to be created once on each Solr node for the life of that process.
 */
public class SolrNodeKeyPair {

  private final CryptoKeys.RSAKeyPair keyPair;

  public SolrNodeKeyPair(CloudConfig cloudConfig) {
    keyPair = createKeyPair(cloudConfig);
  }

  public CryptoKeys.RSAKeyPair getKeyPair() {
    return keyPair;
  }

  private static CryptoKeys.RSAKeyPair createKeyPair(CloudConfig config) {
    if (config == null) {
      return new CryptoKeys.RSAKeyPair();
    }

    String publicKey = config.getPkiHandlerPublicKeyPath();
    String privateKey = config.getPkiHandlerPrivateKeyPath();

    // If both properties unset, then we fall back to generating a new key pair
    if (StrUtils.isNullOrEmpty(publicKey) && StrUtils.isNullOrEmpty(privateKey)) {
      return new CryptoKeys.RSAKeyPair();
    }

    try {
      return new CryptoKeys.RSAKeyPair(new URL(privateKey), new URL(publicKey));
    } catch (IOException | InvalidKeySpecException e) {
      throw new RuntimeException("Bad PublicKeyHandler configuration.", e);
    }
  }
}
