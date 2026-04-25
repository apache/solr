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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.SignedJWT;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.security.jwt.JWTIssuerConfig.HttpsJwksFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests the multi jwks resolver that can fetch keys from multiple JWKs */
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class IssuerAwareJWSKeySelectorTest extends SolrTestCaseJ4 {
  private IssuerAwareJWSKeySelector resolver;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private JWTIssuerConfig.JwkSetFetcher firstJwkList;
  @Mock private JWTIssuerConfig.JwkSetFetcher secondJwkList;
  @Mock private HttpsJwksFactory httpsJwksFactory;

  private KeyHolder k1;
  private KeyHolder k2;
  private KeyHolder k3;
  private KeyHolder k4;
  private KeyHolder k5;
  private List<JWK> keysToReturnFromSecondJwk;
  private Iterator<List<JWK>> refreshSequenceForSecondJwk;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    k1 = new KeyHolder("k1");
    k2 = new KeyHolder("k2");
    k3 = new KeyHolder("k3");
    k4 = new KeyHolder("k4");
    k5 = new KeyHolder("k5");

    when(firstJwkList.getKeys()).thenReturn(asList(k1.getJwk(), k2.getJwk()));
    doAnswer(
            invocation -> {
              keysToReturnFromSecondJwk = refreshSequenceForSecondJwk.next();
              System.out.println("Refresh called, next to return is " + keysToReturnFromSecondJwk);
              return null;
            })
        .when(secondJwkList)
        .refresh();
    when(secondJwkList.getKeys())
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
    resolver = new IssuerAwareJWSKeySelector(Arrays.asList(issuerConfig), true);

    assumeWorkingMockito();
  }

  @Test
  public void findKeyFromFirstList() throws Exception {
    refreshSequenceForSecondJwk =
        asList(asList(k3.getJwk(), k4.getJwk()), asList(k5.getJwk())).iterator();
    resolver.selectJWSKeys(k1.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
    resolver.selectJWSKeys(k2.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
    resolver.selectJWSKeys(k3.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
    resolver.selectJWSKeys(k4.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
    // Key k5 is not in cache, so a refresh will be done
    resolver.selectJWSKeys(k5.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
  }

  @Test(expected = KeySourceException.class)
  public void notFoundKey() throws Exception {
    refreshSequenceForSecondJwk =
        asList(asList(k3.getJwk()), asList(k4.getJwk()), asList(k5.getJwk())).iterator();
    // Will not find key since first refresh returns k4, and we only try one refresh.
    resolver.selectJWSKeys(k5.getJwsHeader(), new IssuerAwareJWSKeySelector.IssuerContext("foo"));
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class KeyHolder {
    private final RSAKey key;
    private final String kid;

    public KeyHolder(String kid) throws JOSEException {
      this.kid = kid;
      key = new RSAKeyGenerator(2048).keyID(kid).generate();
    }

    public RSAKey getRsaKey() {
      return key;
    }

    public JWK getJwk() {
      return key.toPublicJWK();
    }

    public JWSHeader getJwsHeader() {
      return new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build();
    }

    /** Returns a fully signed JWT for use in integration-style tests. */
    public SignedJWT getSignedJWT() throws Exception {
      SignedJWT jwt =
          new SignedJWT(
              new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build(),
              JWTAuthPluginTest.generateClaims());
      jwt.sign(new RSASSASigner(key));
      return jwt;
    }
  }
}
