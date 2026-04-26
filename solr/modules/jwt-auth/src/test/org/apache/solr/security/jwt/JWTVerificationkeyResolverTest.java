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

import static java.util.Arrays.asList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.security.Key;
import java.security.interfaces.ECPublicKey;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.security.jwt.JWTIssuerConfig.HttpsJwksFactory;
import org.jose4j.jwk.EcJwkGenerator;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests the multi jwks resolver that can fetch keys from multiple JWKs */
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class JWTVerificationkeyResolverTest extends SolrTestCaseJ4 {
  private JWTVerificationkeyResolver resolver;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private HttpsJwks firstJwkList;
  @Mock private HttpsJwks secondJwkList;
  @Mock private HttpsJwksFactory httpsJwksFactory;

  private KeyHolder k1;
  private KeyHolder k2;
  private KeyHolder k3;
  private KeyHolder k4;
  private KeyHolder k5;
  private List<JsonWebKey> keysToReturnFromSecondJwk;
  private Iterator<List<JsonWebKey>> refreshSequenceForSecondJwk;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    k1 = new KeyHolder("k1");
    k2 = new KeyHolder("k2");
    k3 = new KeyHolder("k3");
    k4 = new KeyHolder("k4");
    k5 = new KeyHolder("k5");

    when(firstJwkList.getJsonWebKeys()).thenReturn(asList(k1.getJwk(), k2.getJwk()));
    doAnswer(
            invocation -> {
              keysToReturnFromSecondJwk = refreshSequenceForSecondJwk.next();
              System.out.println("Refresh called, next to return is " + keysToReturnFromSecondJwk);
              return null;
            })
        .when(secondJwkList)
        .refresh();
    when(secondJwkList.getJsonWebKeys())
        .then(
            inv -> {
              if (keysToReturnFromSecondJwk == null)
                keysToReturnFromSecondJwk = refreshSequenceForSecondJwk.next();
              return keysToReturnFromSecondJwk;
            });
    when(httpsJwksFactory.createList(ArgumentMatchers.anyList()))
        .thenReturn(asList(firstJwkList, secondJwkList));

    JWTIssuerConfig issuerConfig =
        new JWTIssuerConfig("primary").setIss("foo").setJwksUrl(asList("url1", "url2"));
    JWTIssuerConfig.setHttpsJwksFactory(httpsJwksFactory);
    resolver = new JWTVerificationkeyResolver(Arrays.asList(issuerConfig), true);

    assumeWorkingMockito();
  }

  @Test
  public void findKeyFromFirstList() throws JoseException {
    refreshSequenceForSecondJwk =
        asList(asList(k3.getJwk(), k4.getJwk()), asList(k5.getJwk())).iterator();
    resolver.resolveKey(k1.getJws(), null);
    resolver.resolveKey(k2.getJws(), null);
    resolver.resolveKey(k3.getJws(), null);
    resolver.resolveKey(k4.getJws(), null);
    // Key k5 is not in cache, so a refresh will be done, which
    resolver.resolveKey(k5.getJws(), null);
  }

  @Test(expected = UnresolvableKeyException.class)
  public void notFoundKey() throws JoseException {
    refreshSequenceForSecondJwk =
        asList(asList(k3.getJwk()), asList(k4.getJwk()), asList(k5.getJwk())).iterator();
    // Will not find key since first refresh returns k4, and we only try one refresh.
    resolver.resolveKey(k5.getJws(), null);
  }

  @Test
  public void noIssRequireIssuerFalseSingleIssuerFallback() throws Exception {
    // null iss, requireIssuer=false, single issuer → falls back to that issuer
    when(httpsJwksFactory.createList(ArgumentMatchers.anyList())).thenReturn(asList(firstJwkList));
    JWTIssuerConfig singleIssuerConfig = new JWTIssuerConfig("single").setJwksUrl(asList("url1"));
    resolver = new JWTVerificationkeyResolver(Arrays.asList(singleIssuerConfig), false);

    Key key = resolver.resolveKey(makeJws(k1, claimsWithNoIss()), null);
    assertNotNull(key);
  }

  @Test(expected = SolrException.class)
  public void noIssRequireIssuerFalseMultipleIssuersThrows() throws Exception {
    // null iss, requireIssuer=false, multiple issuers → SolrException (ambiguous)
    JWTIssuerConfig iss1 = new JWTIssuerConfig("iss1").setIss("A").setJwksUrl(asList("url1"));
    JWTIssuerConfig iss2 = new JWTIssuerConfig("iss2").setIss("B").setJwksUrl(asList("url2"));
    resolver = new JWTVerificationkeyResolver(Arrays.asList(iss1, iss2), false);
    resolver.resolveKey(makeJws(k1, claimsWithNoIss()), null);
  }

  @Test
  public void issMismatchSingleIssuerBackCompatFallback() throws Exception {
    // iss present but unrecognised, single issuer → back-compat fallback to that issuer
    when(httpsJwksFactory.createList(ArgumentMatchers.anyList())).thenReturn(asList(firstJwkList));
    JWTIssuerConfig singleIssuerConfig =
        new JWTIssuerConfig("single").setIss("A").setJwksUrl(asList("url1"));
    resolver = new JWTVerificationkeyResolver(Arrays.asList(singleIssuerConfig), true);

    Key key = resolver.resolveKey(makeJws(k1, claimsWithIss("UNKNOWN")), null);
    assertNotNull(key);
  }

  @Test(expected = UnresolvableKeyException.class)
  public void issMismatchMultipleIssuersThrows() throws Exception {
    // iss present but unrecognised, multiple issuers → UnresolvableKeyException
    JWTIssuerConfig iss1 = new JWTIssuerConfig("iss1").setIss("A").setJwksUrl(asList("url1"));
    JWTIssuerConfig iss2 = new JWTIssuerConfig("iss2").setIss("B").setJwksUrl(asList("url2"));
    resolver = new JWTVerificationkeyResolver(Arrays.asList(iss1, iss2), true);
    resolver.resolveKey(makeJws(k1, claimsWithIss("UNKNOWN")), null);
  }

  @Test
  public void ecKeyTypeMaterialisedCorrectly() throws Exception {
    // EC key type should be returned as ECPublicKey, not RSAPublicKey
    EllipticCurveJsonWebKey ecKey = EcJwkGenerator.generateJwk(EllipticCurves.P256);
    ecKey.setKeyId("ec1");
    JsonWebKey ecPublicKey = JsonWebKey.Factory.newJwk(ecKey.getECPublicKey());
    ecPublicKey.setKeyId("ec1");
    JWTIssuerConfig ecIssuerConfig =
        new JWTIssuerConfig("ec-issuer")
            .setIss("ec-iss")
            .setJsonWebKeySet(new JsonWebKeySet(ecPublicKey));
    resolver = new JWTVerificationkeyResolver(Arrays.asList(ecIssuerConfig), false);

    JsonWebSignature ecJws = new JsonWebSignature();
    ecJws.setPayload(claimsWithIss("ec-iss").toJson());
    ecJws.setKey(ecKey.getPrivateKey());
    ecJws.setKeyIdHeaderValue("ec1");
    ecJws.setAlgorithmHeaderValue(AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256);

    Key key = resolver.resolveKey(ecJws, null);
    assertNotNull(key);
    assertTrue(key instanceof ECPublicKey);
  }

  private static JwtClaims claimsWithNoIss() {
    JwtClaims claims = new JwtClaims();
    claims.setExpirationTimeMinutesInTheFuture(10);
    return claims;
  }

  private static JwtClaims claimsWithIss(String iss) {
    JwtClaims claims = claimsWithNoIss();
    claims.setIssuer(iss);
    return claims;
  }

  private static JsonWebSignature makeJws(KeyHolder keyHolder, JwtClaims claims)
      throws JoseException {
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(keyHolder.getRsaKey().getPrivateKey());
    jws.setKeyIdHeaderValue(keyHolder.getRsaKey().getKeyId());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    return jws;
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class KeyHolder {
    private final RsaJsonWebKey key;
    private final String kid;

    public KeyHolder(String kid) throws JoseException {
      key = generateKey(kid);
      this.kid = kid;
    }

    public RsaJsonWebKey getRsaKey() {
      return key;
    }

    public JsonWebKey getJwk() throws JoseException {
      JsonWebKey jsonKey = JsonWebKey.Factory.newJwk(key.getRsaPublicKey());
      jsonKey.setKeyId(kid);
      return jsonKey;
    }

    public JsonWebSignature getJws() {
      JsonWebSignature jws = new JsonWebSignature();
      jws.setPayload(JWTAuthPluginTest.generateClaims().toJson());
      jws.setKey(getRsaKey().getPrivateKey());
      jws.setKeyIdHeaderValue(getRsaKey().getKeyId());
      jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
      return jws;
    }

    private RsaJsonWebKey generateKey(String kid) throws JoseException {
      RsaJsonWebKey rsaJsonWebKey = RsaJwkGenerator.generateJwk(2048);
      rsaJsonWebKey.setKeyId(kid);
      return rsaJsonWebKey;
    }
  }
}
