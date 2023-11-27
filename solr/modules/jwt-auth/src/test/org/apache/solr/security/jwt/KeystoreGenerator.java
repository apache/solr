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

import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.AttributeTypeAndValue;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

public class KeystoreGenerator {

    private static final String PASS_PHRASE = "secret";

    public void generateKeystore(Path existingKeystore, Path newKeystore, String cn) {
        KeyStore ks = null;
        try(FileInputStream fis = new FileInputStream(existingKeystore.toFile())) {
            ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(fis, PASS_PHRASE.toCharArray());
            ks.setCertificateEntry(cn, selfSignCertificate(cn));

        } catch (KeyStoreException |CertificateException |NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
        try (OutputStream fos = new FileOutputStream(newKeystore.toFile())) {
            ks.store(fos, PASS_PHRASE.toCharArray());
        } catch (CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private X509Certificate selfSignCertificate(String commonName) {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(1024);
            KeyPair kp = kpg.generateKeyPair();
            Provider bcp = new BouncyCastleProvider();
            Security.addProvider(bcp);

            X500Name cn = new X500Name(new RDN[]{new RDN(new AttributeTypeAndValue(BCStyle.CN, new DERUTF8String(commonName)))});
            X500Name issuer = new X500Name(new RDN[]{new RDN(new AttributeTypeAndValue(BCStyle.CN, new DERUTF8String("Solr Root CA")))});

            Instant yesterday = LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
            Instant oneYear = ZonedDateTime.ofInstant(yesterday, ZoneOffset.UTC).plusYears(1).toInstant();
            BigInteger serial = new BigInteger(String.valueOf(yesterday.toEpochMilli()));

            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(issuer, serial,
                    Date.from(yesterday), Date.from(oneYear), cn, kp.getPublic());
            BasicConstraints basicConstraints = new BasicConstraints(true);
            certBuilder.addExtension(Extension.basicConstraints, true, basicConstraints);

            ContentSigner cs = new JcaContentSignerBuilder("SHA256WithRSA").build(kp.getPrivate());
            return new JcaX509CertificateConverter().setProvider(bcp).getCertificate(certBuilder.build(cs));
        } catch (CertificateException | CertIOException | NoSuchAlgorithmException | OperatorCreationException e) {
            throw new RuntimeException(e);
        }
    }
}
