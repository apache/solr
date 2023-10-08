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
package org.apache.solr.client.solrj.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import javax.net.ssl.HostnameVerifier;
import org.apache.http.HttpEntity;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.lucene.tests.util.TestRuleRestoreSystemProperties;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SocketFactoryRegistryProvider;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class HttpClientUtilTest extends SolrTestCase {

  @Rule
  public TestRule syspropRestore =
      new TestRuleRestoreSystemProperties(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);

  @After
  public void resetHttpClientBuilder() {
    HttpClientUtil.resetHttpClientBuilder();
  }

  @Test
  public void testSSLSystemProperties() {

    assertNotNull(
        "HTTPS scheme could not be created using system defaults",
        HttpClientUtil.getSocketFactoryRegistryProvider()
            .getSocketFactoryRegistry()
            .lookup("https"));

    assertSSLHostnameVerifier(
        DefaultHostnameVerifier.class, HttpClientUtil.getSocketFactoryRegistryProvider());

    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "true");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(
        DefaultHostnameVerifier.class, HttpClientUtil.getSocketFactoryRegistryProvider());

    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(
        DefaultHostnameVerifier.class, HttpClientUtil.getSocketFactoryRegistryProvider());

    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "false");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(
        NoopHostnameVerifier.class, HttpClientUtil.getSocketFactoryRegistryProvider());
  }

  private void assertSSLHostnameVerifier(
      Class<? extends HostnameVerifier> expected, SocketFactoryRegistryProvider provider) {
    ConnectionSocketFactory socketFactory = provider.getSocketFactoryRegistry().lookup("https");
    assertNotNull("unable to lookup https", socketFactory);
    assertTrue(
        "socketFactory is not an SSLConnectionSocketFactory: " + socketFactory.getClass(),
        socketFactory instanceof SSLConnectionSocketFactory);
    SSLConnectionSocketFactory sslSocketFactory = (SSLConnectionSocketFactory) socketFactory;
    Object hostnameVerifier = getHostnameVerifier(sslSocketFactory);
    assertNotNull("sslSocketFactory has null hostnameVerifier", hostnameVerifier);
    assertEquals(
        "sslSocketFactory does not have expected hostnameVerifier impl",
        expected,
        hostnameVerifier.getClass());
  }

  @SuppressForbidden(reason = "Uses commons-lang3 FieldUtils.readField to get hostnameVerifier")
  private Object getHostnameVerifier(SSLConnectionSocketFactory sslSocketFactory) {
    try {
      return org.apache.commons.lang3.reflect.FieldUtils.readField(
          sslSocketFactory, "hostnameVerifier", true);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Unexpected access error reading hostnameVerifier field", e);
    }
  }

  @Test
  public void testToBooleanDefaultIfNull() throws Exception {
    assertFalse(HttpClientUtil.toBooleanDefaultIfNull(Boolean.FALSE, true));
    assertTrue(HttpClientUtil.toBooleanDefaultIfNull(Boolean.TRUE, false));
    assertFalse(HttpClientUtil.toBooleanDefaultIfNull(null, false));
    assertTrue(HttpClientUtil.toBooleanDefaultIfNull(null, true));
  }

  @Test
  public void testToBooleanObject() {
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("true"));
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("TRUE"));
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("tRuE"));

    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("false"));
    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("FALSE"));
    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("fALSE"));

    assertNull(HttpClientUtil.toBooleanObject("t"));
    assertNull(HttpClientUtil.toBooleanObject("f"));
    assertNull(HttpClientUtil.toBooleanObject("foo"));
    assertNull(HttpClientUtil.toBooleanObject(null));
  }

  @Test
  public void testNonRepeatableMalformedGzipEntityAutoClosed() throws IOException {
    HttpEntity baseEntity =
        new InputStreamEntity(
            new ByteArrayInputStream("this is not compressed".getBytes(StandardCharsets.UTF_8)));
    HttpClientUtil.GzipDecompressingEntity gzipDecompressingEntity =
        new HttpClientUtil.GzipDecompressingEntity(baseEntity);
    Throwable error =
        expectThrows(
            IOException.class,
            "An IOException wrapping a ZIPException should be thrown when loading a malformed GZIP Entity Content",
            gzipDecompressingEntity::getContent);
    assertEquals(
        "IOException should be caused by a ZipException",
        ZipException.class,
        error.getCause() == null ? null : error.getCause().getClass());
    assertNull(
        "The second time getContent is called, null should be returned since the underlying entity is non-repeatable",
        gzipDecompressingEntity.getContent());
    assertEquals(
        "No more content should be available after the GZIP Entity failed to load",
        0,
        baseEntity.getContent().available());
  }

  @Test
  public void testRepeatableMalformedGzipEntity() throws IOException {
    HttpEntity baseEntity = new StringEntity("this is not compressed");
    HttpClientUtil.GzipDecompressingEntity gzipDecompressingEntity =
        new HttpClientUtil.GzipDecompressingEntity(baseEntity);
    Throwable error =
        expectThrows(
            IOException.class,
            "An IOException wrapping a ZIPException should be thrown when loading a malformed GZIP Entity Content",
            gzipDecompressingEntity::getContent);
    assertEquals(
        "IOException should be caused by a ZipException",
        ZipException.class,
        error.getCause() == null ? null : error.getCause().getClass());
    error =
        expectThrows(
            IOException.class,
            "An IOException should be thrown again when re-loading a repeatable malformed GZIP Entity Content",
            gzipDecompressingEntity::getContent);
    assertEquals(
        "IOException should be caused by a ZipException",
        ZipException.class,
        error.getCause() == null ? null : error.getCause().getClass());
  }

  @Test
  public void testRepeatableGzipEntity() throws IOException {
    String testString = "this is compressed";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
      gzipOutputStream.write(testString.getBytes(StandardCharsets.UTF_8));
    }
    // Use an ByteArrayEntity because it is repeatable
    HttpEntity baseEntity = new ByteArrayEntity(baos.toByteArray());
    HttpClientUtil.GzipDecompressingEntity gzipDecompressingEntity =
        new HttpClientUtil.GzipDecompressingEntity(baseEntity);
    try (InputStream stream = gzipDecompressingEntity.getContent()) {
      assertEquals(
          "Entity incorrect after decompression",
          testString,
          new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
    try (InputStream stream = gzipDecompressingEntity.getContent()) {
      assertEquals(
          "Entity incorrect after decompression after repeating",
          testString,
          new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testNonRepeatableGzipEntity() throws IOException {
    String testString = "this is compressed";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
      gzipOutputStream.write(testString.getBytes(StandardCharsets.UTF_8));
    }
    // Use an InputStreamEntity because it is non-repeatable
    HttpEntity baseEntity = new InputStreamEntity(new ByteArrayInputStream(baos.toByteArray()));
    HttpClientUtil.GzipDecompressingEntity gzipDecompressingEntity =
        new HttpClientUtil.GzipDecompressingEntity(baseEntity);
    try (InputStream stream = gzipDecompressingEntity.getContent()) {
      assertEquals(
          "Entity incorrect after decompression",
          testString,
          new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
    try (InputStream stream = gzipDecompressingEntity.getContent()) {
      expectThrows(
          IOException.class,
          "Entity Content should already be closed since the input is non-repeatable",
          stream::available);
    }
  }
}
