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
package org.apache.solr.cloud;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.CryptoKeys;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class TestRSAKeyPair extends SolrTestCase {
  @Test
  public void testGenKeyPair() throws Exception {
    testRoundTrip(new CryptoKeys.RSAKeyPair());
  }

  @Test
  public void testReadKeysFromDisk() throws Exception {
    URL privateKey = getClass().getClassLoader().getResource("cryptokeys/priv_key512_pkcs8.pem");
    URL publicKey = getClass().getClassLoader().getResource("cryptokeys/pub_key512.der");
    assertNotNull(privateKey);
    assertNotNull(publicKey);
    testRoundTrip(new CryptoKeys.RSAKeyPair(privateKey, publicKey));
  }

  private void testRoundTrip(CryptoKeys.RSAKeyPair kp) throws Exception {
    int keySizeInBytes = kp.getKeySizeInBytes();
    // Max size of the plaintext can only be as big as the key in bytes with no padding
    String plaintextString = TestUtil.randomSimpleString(random(), keySizeInBytes);
    final byte[] plaintext = plaintextString.getBytes(StandardCharsets.UTF_8);

    byte[] encrypted = kp.encrypt(ByteBuffer.wrap(plaintext));
    MatcherAssert.assertThat(plaintext, not(equalTo(encrypted)));

    byte[] decrypted = CryptoKeys.decryptRSA(encrypted, kp.getPublicKey());

    assertTrue(
        "Decrypted text is shorter than original text.", decrypted.length >= plaintext.length);

    // Strip off any null bytes RSAKeyPair uses RSA/ECB/NoPadding and during decryption null bytes
    // can be left.
    // Under "Known Limitations"
    // https://www.ibm.com/docs/en/sdk-java-technology/8?topic=guide-ibmjceplus-ibmjceplusfips-providers
    assertArrayEquals(plaintext, Arrays.copyOf(decrypted, plaintext.length));
  }
}
