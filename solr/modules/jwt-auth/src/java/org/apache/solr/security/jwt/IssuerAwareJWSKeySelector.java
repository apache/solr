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
package org.apache.solr.security.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKMatcher;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Key;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLHandshakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves JWS signature verification keys from a set of {@link JWTIssuerConfig} objects, which may
 * represent any valid configuration in Solr's security.json, i.e. static list of JWKs or keys
 * retrieved from HTTPS JWK endpoints.
 *
 * <p>This implementation maintains a map of issuers, each with its own list of {@link JWK}, and
 * resolves the correct key from the correct issuer. The issuer is passed in via {@link
 * IssuerContext}.
 *
 * <p>If a key is not found and the issuer is backed by HTTPS JWKs, one cache refresh is attempted
 * before failing.
 */
public class IssuerAwareJWSKeySelector implements JWSKeySelector<SecurityContext> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, JWTIssuerConfig> issuerConfigs = new HashMap<>();
  private final boolean requireIssuer;

  /**
   * SecurityContext subclass that carries the (unverified) issuer claim from the JWT payload,
   * allowing the key selector to look up the correct issuer configuration.
   */
  public static class IssuerContext implements SecurityContext {
    private final String issuer;

    public IssuerContext(String issuer) {
      this.issuer = issuer;
    }

    public String getIssuer() {
      return issuer;
    }
  }

  /**
   * Resolves key from a JWKs from one or more IssuerConfigs
   *
   * @param issuerConfigs Collection of configuration objects for the issuer(s)
   * @param requireIssuer if true, will require 'iss' claim on jws
   */
  public IssuerAwareJWSKeySelector(
      Collection<JWTIssuerConfig> issuerConfigs, boolean requireIssuer) {
    this.requireIssuer = requireIssuer;
    issuerConfigs.forEach(ic -> this.issuerConfigs.put(ic.getIss(), ic));
  }

  @Override
  public List<? extends Key> selectJWSKeys(JWSHeader header, SecurityContext context)
      throws KeySourceException {
    String tokenIssuer =
        (context instanceof IssuerContext) ? ((IssuerContext) context).getIssuer() : null;

    JWTIssuerConfig issuerConfig = resolveIssuerConfig(tokenIssuer);

    List<JWK> allJwks = new ArrayList<>();
    String keysSource = "N/A";
    try {
      if (issuerConfig.usesHttpsJwk()) {
        keysSource = "[" + String.join(", ", issuerConfig.getJwksUrls()) + "]";
        for (JWTIssuerConfig.JwkSetFetcher fetcher : issuerConfig.getHttpsJwks()) {
          try {
            allJwks.addAll(fetcher.getKeys());
          } catch (SSLHandshakeException e) {
            throw new KeySourceException(
                "Failed to connect with "
                    + fetcher.getLocation()
                    + ", do you have the correct SSL certificate configured?",
                e);
          }
        }
      } else {
        keysSource = "static list of keys in security.json";
        allJwks.addAll(issuerConfig.getJsonWebKeySet().getKeys());
      }
    } catch (IOException | ParseException e) {
      throw new KeySourceException(
          String.format(
              Locale.ROOT, "Unable to fetch JWKs from source %s: %s", keysSource, e.getMessage()),
          e);
    }

    JWKSelector selector = new JWKSelector(JWKMatcher.forJWSHeader(header));
    List<JWK> matchingJwks = selector.select(new JWKSet(allJwks));

    if (matchingJwks.isEmpty() && issuerConfig.usesHttpsJwk()) {
      if (log.isDebugEnabled()) {
        log.debug(
            "No matching key found for JWS header {} in {} keys from {}; refreshing",
            header,
            allJwks.size(),
            keysSource);
      }
      allJwks.clear();
      try {
        for (JWTIssuerConfig.JwkSetFetcher fetcher : issuerConfig.getHttpsJwks()) {
          fetcher.refresh();
          allJwks.addAll(fetcher.getKeys());
        }
      } catch (IOException | ParseException e) {
        throw new KeySourceException("Failed to refresh JWKs from " + keysSource + ": " + e, e);
      }
      matchingJwks = selector.select(new JWKSet(allJwks));
    }

    if (matchingJwks.isEmpty()) {
      throw new KeySourceException(
          String.format(
              Locale.ROOT,
              "Unable to find a suitable verification key for JWS w/ header %s from %d keys from source %s",
              header,
              allJwks.size(),
              keysSource));
    }

    List<Key> keys = new ArrayList<>();
    for (JWK jwk : matchingJwks) {
      try {
        if (jwk instanceof RSAKey) {
          keys.add(((RSAKey) jwk).toPublicKey());
        } else if (jwk instanceof ECKey) {
          keys.add(((ECKey) jwk).toPublicKey());
        } else if (jwk instanceof OctetSequenceKey) {
          keys.add(((OctetSequenceKey) jwk).toSecretKey());
        } else {
          log.warn("Unsupported JWK type: {}", jwk.getKeyType());
        }
      } catch (JOSEException e) {
        log.warn("Failed to convert JWK to Key", e);
      }
    }

    if (keys.isEmpty()) {
      throw new KeySourceException(
          "Could not extract a usable public key from matched JWK(s) for header " + header);
    }

    return keys;
  }

  private JWTIssuerConfig resolveIssuerConfig(String tokenIssuer) throws KeySourceException {
    if (tokenIssuer == null) {
      if (requireIssuer) {
        throw new KeySourceException("Token does not contain required issuer claim");
      } else if (issuerConfigs.size() == 1) {
        return issuerConfigs.values().iterator().next();
      } else {
        throw new KeySourceException(
            "Signature verification not supported for multiple issuers without 'iss' claim in token.");
      }
    } else {
      JWTIssuerConfig config = issuerConfigs.get(tokenIssuer);
      if (config == null) {
        if (issuerConfigs.size() > 1) {
          throw new KeySourceException(
              "No issuers configured for iss='" + tokenIssuer + "', cannot validate signature");
        } else if (issuerConfigs.size() == 1) {
          config = issuerConfigs.values().iterator().next();
          log.debug(
              "No issuer matching token's iss claim, but exactly one configured, selecting that one");
        } else {
          throw new KeySourceException(
              "Signature verification failed due to no configured issuer with id " + tokenIssuer);
        }
      }
      return config;
    }
  }

  Set<JWTIssuerConfig> getIssuerConfigs() {
    return new HashSet<>(issuerConfigs.values());
  }
}
