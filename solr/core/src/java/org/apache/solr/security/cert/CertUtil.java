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
package org.apache.solr.security.cert;

import java.lang.invoke.MethodHandles;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for certificate-related operations, including extracting fields from the subject or
 * issuer DN and SAN fields from X509 certificates.
 */
public class CertUtil {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SUBJECT_DN_PREFIX = "subject.dn";
  public static final String ISSUER_DN_PREFIX = "issuer.dn";
  public static final String SAN_PREFIX = "san.";

  /**
   * Extracts a specified field or the entire DN from an X500Principal, such as a certificate's
   * subject or issuer. If the entire DN is returned the format would be RFC2253
   *
   * @param principal The X500Principal from which to extract information.
   * @param path The DN field to extract, or a prefix indicating the entire DN.
   * @return The value of the specified field, or the entire DN if just a prefix is provided.
   */
  public static Optional<String> extractFieldFromX500Principal(
      X500Principal principal, String path) {
    if (principal == null || path == null || path.isEmpty()) {
      return Optional.empty();
    }

    String dn = principal.getName(X500Principal.RFC2253);

    try {
      final LdapName ln = new LdapName(dn);
      // Path does not specify a field, return the whole DN after normalizing it
      if (path.equalsIgnoreCase(SUBJECT_DN_PREFIX) || path.equalsIgnoreCase(ISSUER_DN_PREFIX)) {
        // Remove whitespaces around RDNs, sort them, and reconstruct the DN string
        List<String> rdns =
            ln.getRdns().stream()
                .map(rdn -> rdn.getType().trim() + "=" + rdn.getValue().toString().trim())
                .sorted()
                .collect(Collectors.toList());
        dn = String.join(",", rdns);
        return Optional.of(dn);
      }

      // Extract and return the specified DN field value
      String field = null;
      if (path.startsWith(SUBJECT_DN_PREFIX + ".")) {
        field = path.substring((SUBJECT_DN_PREFIX + ".").length());
      } else if (path.startsWith(ISSUER_DN_PREFIX + ".")) {
        field = path.substring((ISSUER_DN_PREFIX + ".").length());
      }

      if (field != null) {
        String fieldF = field;
        return ln.getRdns().stream()
            .filter(rdn -> rdn.getType().equalsIgnoreCase(fieldF))
            .findFirst()
            .map(Rdn::getValue)
            .map(Object::toString);
      }
    } catch (InvalidNameException e) {
      log.warn("Invalid DN in LdapName instantiation. DN={}", dn);
    }
    return Optional.empty();
  }

  /**
   * Extracts a specified field or the entire subject DN from an X509 certificate.
   *
   * @param certificate The certificate from which to extract the subject DN information.
   * @param path The path specifying the subject DN field to extract or a prefix for the entire DN.
   * @return An Optional containing the value of the specified subject DN field or the entire DN;
   *     empty if not found.
   */
  public static Optional<String> extractFromSubjectDN(X509Certificate certificate, String path) {
    return extractFieldFromX500Principal(certificate.getSubjectX500Principal(), path);
  }

  /**
   * Extracts a specified field or the entire issuer DN from an X509 certificate.
   *
   * @param certificate The certificate from which to extract the issuer DN information.
   * @param path The path specifying the issuer DN field to extract or a prefix for the entire DN.
   * @return An Optional containing the value of the specified issuer DN field or the entire DN;
   *     empty if not found.
   */
  public static Optional<String> extractFromIssuerDN(X509Certificate certificate, String path) {
    return extractFieldFromX500Principal(certificate.getIssuerX500Principal(), path);
  }

  /**
   * Extracts SAN (Subject Alternative Name) fields from an X509 certificate that match a specified
   * path and predicate.
   *
   * @param certificate The certificate from which to extract SAN information.
   * @param path The path specifying the SAN field to extract.
   * @param valueMatcher A predicate to apply to each SAN value for filtering.
   * @return An Optional containing a list of SAN values that match the specified path and
   *     predicate; empty if none found.
   * @throws CertificateParsingException If an error occurs while parsing the certificate for SAN
   *     fields.
   */
  public static Optional<List<String>> extractFromSAN(
      X509Certificate certificate, String path, Predicate<String> valueMatcher)
      throws CertificateParsingException {
    Collection<List<?>> altNames = certificate.getSubjectAlternativeNames();
    if (altNames == null) {
      return Optional.empty();
    }
    List<String> filteredSANValues =
        altNames.stream()
            .filter(entry -> entry != null && entry.size() >= 2)
            .filter(
                entry -> {
                  SANType sanType = SANType.fromValue((Integer) entry.get(0));
                  return sanType != null && sanType.pathLowerCase().equals(path);
                })
            .map(entry -> (String) entry.get(1))
            .filter(valueMatcher)
            .collect(Collectors.toList());

    return Optional.of(filteredSANValues);
  }

  /**
   * Supported SAN (Subject Alternative Name) types as defined in <a
   * href="https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.6">RFC 5280</a>
   */
  public enum SANType {
    OTHER_NAME(0), // OtherName
    EMAIL(1), // rfc822Name
    DNS(2), // dNSName
    X400_ADDRESS(3), // x400Address
    DIRECTORY_NAME(4), // directoryName
    EDI_PARTY_NAME(5), // ediPartyName
    URI(6), // uniformResourceIdentifier
    IP_ADDRESS(7), // iPAddress
    REGISTERED_ID(8); // registeredID

    private static final Map<Integer, SANType> lookup =
        EnumSet.allOf(SANType.class).stream()
            .collect(Collectors.toMap(SANType::getValue, sanType -> sanType));

    private final int value;

    SANType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static SANType fromValue(int value) {
      return lookup.get(value);
    }

    public String path() {
      return "SAN." + name();
    }

    public String pathLowerCase() {
      return path().toLowerCase(Locale.ROOT);
    }
  }
}
