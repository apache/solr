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

package org.apache.solr.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.spec.InvalidKeySpecException;

public class PublicKeyHandler extends RequestHandlerBase {
  public static final String PATH = "/admin/info/key";

  //This is an optimization for tests only
  public static volatile CryptoKeys.RSAKeyPair REUSABLE_KEYPAIR ;
  final CryptoKeys.RSAKeyPair keyPair;

  @VisibleForTesting
  public PublicKeyHandler() throws IOException, InvalidKeySpecException {
    keyPair = createKeyPair(null);
  }

  public PublicKeyHandler(CloudConfig config) throws IOException, InvalidKeySpecException {
    keyPair = createKeyPair(config);
  }

  private static CryptoKeys.RSAKeyPair createKeyPair(CloudConfig config) throws IOException, InvalidKeySpecException {
    CryptoKeys.RSAKeyPair reused = REUSABLE_KEYPAIR;
    if(reused != null) return reused;
    if (config == null) {
      return new CryptoKeys.RSAKeyPair();
    }

    String publicKey = config.getPkiHandlerPublicKeyPath();
    String privateKey = config.getPkiHandlerPrivateKeyPath();

    // If both properties unset, then we fall back to generating a new key pair
    if (StringUtils.isEmpty(publicKey) && StringUtils.isEmpty(privateKey)) {
      return new CryptoKeys.RSAKeyPair();
    }

    return new CryptoKeys.RSAKeyPair(toURL(privateKey), toURL(publicKey));
  }

  /**
   * Resolve a configured key location to a URL. The location may be a real URL
   * (e.g. {@code file:...}, set by the test framework) or a bare classpath
   * resource path (e.g. {@code cryptokeys/priv_key512_pkcs8.pem}, the fallback
   * baked into the MiniSolrCloudCluster solr.xml template). A bare path is
   * resolved against the classloader rather than blindly fed to {@code new URL()},
   * which would throw MalformedURLException ("no protocol").
   */
  private static URL toURL(String location) throws IOException {
    try {
      return new URL(location);
    } catch (MalformedURLException e) {
      URL resource = PublicKeyHandler.class.getClassLoader().getResource(location);
      if (resource == null) {
        throw new IOException("Could not resolve PublicKeyHandler key location as URL or classpath resource: " + location, e);
      }
      return resource;
    }
  }

  public String getPublicKey() {
    return keyPair.getPublicKeyStr();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.add("key", keyPair.getPublicKeyStr());
  }

  @Override
  public String getDescription() {
    return "Return the public key of this server";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
