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

package org.apache.solr.util.configuration.providers;

import static org.apache.solr.util.configuration.providers.AbstractSSLCredentialProvider.DEFAULT_CREDENTIAL_KEY_MAP;
import static org.hamcrest.core.Is.is;

import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.configuration.SSLCredentialProvider;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** */
public class EnvSSLCredentialProviderTest extends SolrTestCase {

  @Test
  public void testGetCredentials() {
    int cnt = 0;
    Map<String, String> envvars =
        Map.of(
            EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, "pw" + ++cnt,
            EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, "pw" + ++cnt,
            EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, "pw" + ++cnt,
            EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, "pw" + ++cnt);
    EnvSSLCredentialProvider sut = new EnvSSLCredentialProvider();
    sut.setEnvVars(envvars);
    cnt = 0;
    for (Map.Entry<SSLCredentialProvider.CredentialType, String> set :
        DEFAULT_CREDENTIAL_KEY_MAP.entrySet()) {
      String expectedpw = "pw" + ++cnt;
      MatcherAssert.assertThat(sut.getCredential(set.getKey()), is(expectedpw));
    }
  }

  @Test
  public void testGetCredentialsWithEnvVars() {
    EnvSSLCredentialProvider sut = new EnvSSLCredentialProvider();
    // assuming not to fail
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_KEY_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_CLIENT_KEY_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_TRUST_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_CLIENT_TRUST_STORE_PASSWORD);
  }
}
