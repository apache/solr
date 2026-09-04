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

import java.security.Principal;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;

/**
 * Defines the interface for resolving a {@link Principal} from an X509 certificate. Implementations
 * of this interface are responsible for extracting a specific piece of information from the
 * certificate and converting it into a {@link Principal}.
 */
public interface CertPrincipalResolver {
  /**
   * Resolves a {@link Principal} from the given X509 certificate.
   *
   * <p>This method is intended to extract principal information, such as a common name (CN) or an
   * email address, from the specified certificate and encapsulate it into a {@link Principal}
   * object. The specific field or attribute of the certificate to be used as the principal, and the
   * logic for its extraction, is defined by the implementation.
   *
   * @param certificate The X509Certificate from which to resolve the principal.
   * @return A {@link Principal} object representing the resolved principal from the certificate.
   * @throws SSLPeerUnverifiedException If the peer's identity has not been verified.
   * @throws CertificateParsingException If an error occurs while parsing the certificate for
   *     principal information.
   */
  Principal resolvePrincipal(X509Certificate certificate)
      throws SSLPeerUnverifiedException, CertificateParsingException;
}
