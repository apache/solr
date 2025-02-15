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
package org.apache.solr.util;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

public class SolrTestNonSecureRandomProvider extends Provider {

  public SolrTestNonSecureRandomProvider() {
    super("SolrTestNonSecure", "1.0", "A Test only, non secure provider");
    put("SecureRandom.SHA1PRNG", NotSecurePseudoRandomSpi.class.getName());
    put("SecureRandom.NativePRNG", NotSecurePseudoRandomSpi.class.getName());
    put("SecureRandom.DRBG", NotSecurePseudoRandomSpi.class.getName());

    put("SecureRandom.SHA1PRNG ThreadSafe", "true");
    put("SecureRandom.NativePRNG ThreadSafe", "true");
    put("SecureRandom.DRBG ThreadSafe", "true");

    put("SecureRandom.SHA1PRNG ImplementedIn", "Software");
    put("SecureRandom.NativePRNG ImplementedIn", "Software");
    put("SecureRandom.DRBG ImplementedIn", "Software");
  }

  public static void injectProvider() {
    // Install our non secure solr test secure random provider
    Provider[] secureRandomProviders = Security.getProviders("SecureRandom.SHA1PRNG");
    if ((secureRandomProviders == null)
        || (secureRandomProviders.length < 1)
        || (!SolrTestNonSecureRandomProvider.class.equals(secureRandomProviders[0].getClass()))) {
      Security.insertProviderAt(new SolrTestNonSecureRandomProvider(), 1);
    }

    // Assert that new SecureRandom() and
    // SecureRandom.getInstance("SHA1PRNG") return a SecureRandom backed
    // by our non secure test provider.
    SecureRandom rng1 = new SecureRandom();
    if (!SolrTestNonSecureRandomProvider.class.equals(rng1.getProvider().getClass())) {
      throw new SecurityException(
          "new SecureRandom() backed by wrong Provider: " + rng1.getProvider().getClass());
    }

    boolean skipCheck = false;
    SecureRandom rng2 = null;
    try {
      rng2 = SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      skipCheck = true;
    }
    if (!skipCheck
        && !SolrTestNonSecureRandomProvider.class.equals(rng2.getProvider().getClass())) {
      throw new SecurityException(
          "SecureRandom.getInstance(\"SHA1PRNG\") backed by wrong"
              + " Provider: "
              + rng2.getProvider().getClass());
    }
  }
}
