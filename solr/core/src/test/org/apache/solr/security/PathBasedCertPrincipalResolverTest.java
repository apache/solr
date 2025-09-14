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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import java.security.Principal;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.x500.X500Principal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.security.cert.CertResolverPattern;
import org.apache.solr.security.cert.CertUtil.SANType;
import org.apache.solr.security.cert.PathBasedCertPrincipalResolver;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
public class PathBasedCertPrincipalResolverTest extends SolrTestCaseJ4 {

  private static final String SUBJECT_DN =
      " CN=John Doe, O=Solr Corp, OU= Engineering, C=US , ST =California, L =San Francisco"; // whitespaces should be ignored
  private static final String ISSUER_DN =
      "CN = Issuer, O= Issuer Corp, OU =IT, C=US , ST=California, L =San Francisco ";

  private X509Certificate mockCertificate;

  @BeforeClass
  public static void setupMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    mockCertificate = mock(X509Certificate.class);
  }

  @Test
  public void testSubjectDn() throws SSLPeerUnverifiedException, CertificateParsingException {
    // CN
    testCertificateCases(SUBJECT_DN, "subject.dn.CN", "John Doe");

    // O
    testCertificateCases(SUBJECT_DN, "subject.dn.O", "Solr Corp");

    // OU
    testCertificateCases(SUBJECT_DN, "subject.dn.OU", "Engineering");

    // C
    testCertificateCases(SUBJECT_DN, "subject.dn.C", "US");

    // ST
    testCertificateCases(SUBJECT_DN, "subject.dn.ST", "California");

    // L
    testCertificateCases(SUBJECT_DN, "subject.dn.L", "San Francisco");
  }

  @Test
  public void testIssuerDn() throws SSLPeerUnverifiedException, CertificateParsingException {
    // CN
    testCertificateCases(ISSUER_DN, "issuer.dn.CN", "Issuer");

    // O
    testCertificateCases(ISSUER_DN, "issuer.dn.O", "Issuer Corp");

    // OU
    testCertificateCases(ISSUER_DN, "issuer.dn.OU", "IT");

    // C
    testCertificateCases(ISSUER_DN, "issuer.dn.C", "US");

    // ST
    testCertificateCases(ISSUER_DN, "issuer.dn.ST", "California");

    // L
    testCertificateCases(ISSUER_DN, "issuer.dn.L", "San Francisco");
  }

  @Test
  public void testSan() {
    // Email
    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user1@example.com"),
            List.of(SANType.EMAIL.getValue(), "user2@example.com")),
        "san.email",
        "user1@example.com");

    // Email with 'extract'
    testCertificateCases(
        List.of(List.of(SANType.EMAIL.getValue(), "user@example.com")),
        null,
        "san.email",
        List.of("user@example.com"),
        "startsWith",
        Map.of("after", "@", "before", ".com"),
        "example");
    testCertificateCases(
        List.of(List.of(SANType.EMAIL.getValue(), "user@example.com")),
        null,
        "san.email",
        List.of("user@example.com"),
        "equals",
        Map.of("before", "@"),
        "user");

    // DNS
    testCertificateCases(
        List.of(
            List.of(SANType.DNS.getValue(), "value1.example.com"),
            List.of(SANType.DNS.getValue(), "value2.example.com")),
        "san.dns",
        "value1.example.com");

    // URI
    testCertificateCases(
        List.of(
            List.of(SANType.URI.getValue(), "http://example.com"),
            List.of(SANType.URI.getValue(), "http://example2.org")),
        "san.uri",
        List.of("http://example.com"),
        "equals",
        "http://example.com");

    // IP Address
    testCertificateCases(
        List.of(
            List.of(SANType.IP_ADDRESS.getValue(), "192.168.1.1"),
            List.of(SANType.IP_ADDRESS.getValue(), "10.0.0.1")),
        "san.IP_ADDRESS",
        "192.168.1.1");

    // OTHER_NAME
    testCertificateCases(
        List.of(List.of(SANType.OTHER_NAME.getValue(), "1.2.3.4")), "san.OTHER_NAME", "1.2.3.4");

    // X400_ADDRESS
    testCertificateCases(
        List.of(List.of(SANType.X400_ADDRESS.getValue(), "X400AddressValue")),
        "san.X400_ADDRESS",
        "X400AddressValue");

    // DIRECTORY_NAME
    testCertificateCases(
        List.of(List.of(SANType.DIRECTORY_NAME.getValue(), "DirectoryNameValue")),
        "san.DIRECTORY_NAME",
        "DirectoryNameValue");

    // EDI_PARTY_NAME
    testCertificateCases(
        List.of(List.of(SANType.EDI_PARTY_NAME.getValue(), "EdiPartyNameValue")),
        "san.EDI_PARTY_NAME",
        "EdiPartyNameValue");

    // REGISTERED_ID
    testCertificateCases(
        List.of(List.of(SANType.REGISTERED_ID.getValue(), "RegisteredIdValue")),
        "san.REGISTERED_ID",
        "RegisteredIdValue");

    //  CheckType tests
    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user1@example.com"),
            List.of(SANType.EMAIL.getValue(), "user2@example.com")),
        "san.email",
        List.of("user2@example.com"),
        "equals",
        "user2@example.com");

    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user1@example.com"),
            List.of(SANType.EMAIL.getValue(), "user2@example.com")),
        "san.email",
        List.of("user2"),
        "contains",
        "user2@example.com");

    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user1@example.com"),
            List.of(SANType.EMAIL.getValue(), "user2@example.com")),
        "san.email",
        List.of("user2"),
        "startsWith",
        "user2@example.com");

    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user@example1.com"),
            List.of(SANType.EMAIL.getValue(), "user@example2.com")),
        "san.email",
        List.of("example2.com"),
        "endsWith",
        "user@example2.com");

    testCertificateCases(
        List.of(
            List.of(SANType.EMAIL.getValue(), "user1@example.com"),
            List.of(SANType.EMAIL.getValue(), "user2@example.com")),
        "san.email",
        Collections.emptyList(),
        "*",
        "user1@example.com");
  }

  @Test
  public void testMultipleSANEntries()
      throws SSLPeerUnverifiedException, CertificateParsingException {

    when(mockCertificate.getSubjectAlternativeNames())
        .thenReturn(
            List.of(
                List.of(SANType.EMAIL.getValue(), "user1@example.com"),
                List.of(SANType.EMAIL.getValue(), "user2@example.com")));

    Map<String, Object> params =
        Map.of(
            "path",
            "SAN.email",
            "filter",
            Map.of(
                "checkType",
                "equals",
                "values",
                List.of("user1@example.com", "user2@example.com")));

    PathBasedCertPrincipalResolver resolver = new PathBasedCertPrincipalResolver(params);
    Principal result = resolver.resolvePrincipal(mockCertificate);

    assertNotNull(result);
    assertTrue(
        result.getName().contains("user1@example.com")
            || result.getName().contains("user2@example.com"));
  }

  @Test
  public void testResolverWithInvalidPath() {
    Map<String, Object> params =
        Map.of(
            "path",
            "Invalid.path",
            "filter",
            Map.of("checkType", "equals", "values", List.of("value1", "value2")));

    assertThrows(
        IllegalArgumentException.class,
        () -> new PathBasedCertPrincipalResolver(params).resolvePrincipal(mockCertificate));
  }

  @Test
  public void testNoMatchFound() {
    Map<String, Object> params =
        Map.of(
            "path",
            "subject.dn.CN",
            "filter",
            Map.of("checkType", "equals", "values", List.of("NonExistent")));

    PathBasedCertPrincipalResolver resolver = new PathBasedCertPrincipalResolver(params);
    assertThrows(SolrException.class, () -> resolver.resolvePrincipal(mockCertificate));
  }

  @Test
  public void testResolverWithExtractPatternMissing()
      throws SSLPeerUnverifiedException, CertificateParsingException {
    when(mockCertificate.getSubjectAlternativeNames())
        .thenReturn(List.of(List.of(SANType.EMAIL.getValue(), "info@example.com")));

    // Both 'after' and 'before' patterns are missing
    final Map<String, Object> params = new HashMap<>();
    params.put("path", "SAN.email");
    params.put("filter", Map.of("checkType", "startsWith", "values", List.of("info@")));
    assertResolvedPrincipal(params, "info@example.com");

    // Only 'before' pattern is provided, 'after' is missing
    params.put("extract", Map.of("before", ".com"));
    assertResolvedPrincipal(params, "info@example");

    // Only 'after' pattern is provided, 'before' is missing
    params.put("extract", Map.of("after", "@"));
    assertResolvedPrincipal(params, "example.com");

    // Invalid 'after' pattern that doesn't exist in the SAN value
    params.put("extract", Map.of("after", "notfound"));
    assertResolvedPrincipal(
        params,
        "info@example.com"); // Expect the original value since the 'after' pattern was not found
  }

  private void testCertificateCases(List<List<?>> sanData, String path, String expectedValue) {

    testCertificateCases(
        sanData,
        null,
        path,
        Collections.emptyList(),
        CertResolverPattern.CheckType.WILDCARD.toString(),
        Collections.emptyMap(),
        expectedValue);
  }

  private void testCertificateCases(
      List<List<?>> sanData,
      String path,
      List<String> filterValues,
      String filterCheckType,
      String expectedValue) {

    testCertificateCases(
        sanData, null, path, filterValues, filterCheckType, Collections.emptyMap(), expectedValue);
  }

  private void testCertificateCases(String dn, String path, String expectedValue) {

    testCertificateCases(
        null,
        dn,
        path,
        Collections.emptyList(),
        CertResolverPattern.CheckType.WILDCARD.toString(),
        Collections.emptyMap(),
        expectedValue);
  }

  private void testCertificateCases(
      List<List<?>> sanData,
      String dn,
      String path,
      List<String> filterValues,
      String filterCheckType,
      Map<String, String> extractPatterns,
      String expectedValue) {
    try {
      if (path.startsWith("san")) {
        when(mockCertificate.getSubjectAlternativeNames()).thenReturn(sanData);
      } else if (path.startsWith("subject.dn")) {
        X500Principal subjectDN = new X500Principal(dn);
        when(mockCertificate.getSubjectX500Principal()).thenReturn(subjectDN);
      } else if (path.startsWith("issuer.dn")) {
        X500Principal issuerDN = new X500Principal(dn);
        when(mockCertificate.getIssuerX500Principal()).thenReturn(issuerDN);
      }

      Map<String, Object> params = new HashMap<>();
      params.put("path", path);
      params.put("filter", Map.of("checkType", filterCheckType, "values", filterValues));
      params.put("extract", extractPatterns);

      assertResolvedPrincipal(params, expectedValue);
    } catch (CertificateParsingException | SSLPeerUnverifiedException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertResolvedPrincipal(Map<String, Object> params, String expectedPrincipalName)
      throws SSLPeerUnverifiedException, CertificateParsingException {
    PathBasedCertPrincipalResolver resolver = new PathBasedCertPrincipalResolver(params);
    Principal result = resolver.resolvePrincipal(mockCertificate);

    assertNotNull(result);
    assertEquals(expectedPrincipalName, result.getName());
  }
}
