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

package org.apache.solr.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobRepositoryMockingTest extends SolrTestCaseJ4 {

  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private static final String[][] PARSED =
      new String[][] {{"foo", "bar", "baz"}, {"bang", "boom", "bash"}};
  private static final String BLOBSTR = "foo,bar,baz\nbang,boom,bash";
  private CoreContainer mockContainer = mock(CoreContainer.class);

  @SuppressWarnings({"unchecked", "rawtypes"})
  private ConcurrentHashMap<String, BlobRepository.BlobContent> blobStorage;

  BlobRepository repository;
  ByteBuffer blobData = ByteBuffer.wrap(BLOBSTR.getBytes(UTF8));
  boolean blobFetched = false;
  String blobKey = "";
  String url = null;
  ByteBuffer filecontent = null;

  @BeforeClass
  public static void beforeClass() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    blobFetched = false;
    blobKey = "";
    reset(mockContainer);
    blobStorage = new ConcurrentHashMap<>();
    repository =
        new BlobRepository(mockContainer) {
          @Override
          ByteBuffer fetchBlob(String key) {
            blobKey = key;
            blobFetched = true;
            return blobData;
          }

          @Override
          ByteBuffer fetchFromUrl(String key, String url) {
            if (!Objects.equals(url, BlobRepositoryMockingTest.this.url)) return null;
            blobKey = key;
            blobFetched = true;
            return filecontent;
          }

          @Override
          @SuppressWarnings({"rawtypes"})
          ConcurrentHashMap<String, BlobContent> createMap() {
            return blobStorage;
          }
        };
  }

  @Test(expected = SolrException.class)
  public void testCloudOnly() {
    when(mockContainer.isZooKeeperAware()).thenReturn(false);
    try {
      repository.getBlobIncRef("foo!");
    } catch (SolrException e) {
      verify(mockContainer).isZooKeeperAware();
      throw e;
    }
  }

  @Test
  public void testGetBlobIncrRefString() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    BlobRepository.BlobContentRef<ByteBuffer> ref = repository.getBlobIncRef("foo!");
    assertEquals("foo!", blobKey);
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    assertNotNull(blobStorage.get("foo!"));
  }

  @Test
  public void testGetBlobIncrRefByUrl() throws Exception {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    filecontent = TestSolrConfigHandler.getFileContent("runtimecode/runtimelibs_v2.jar.bin");
    url = "http://localhost:8080/myjar/location.jar";
    BlobRepository.BlobContentRef<?> ref =
        repository.getBlobIncRef(
            "filefoo",
            null,
            url,
            "bc5ce45ad281b6a08fb7e529b1eb475040076834816570902acb6ebdd809410e31006efdeaa7f78a6c35574f3504963f5f7e4d92247d0eb4db3fc9abdda5d417");
    assertEquals("filefoo", blobKey);
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(filecontent, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    try {
      repository.getBlobIncRef("filefoo", null, url, "WRONG-SHA512-KEY");
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(" expected sha512 hash : WRONG-SHA512-KEY , actual :"));
    }

    url = null;
    filecontent = null;
  }

  @Test
  public void testCachedAlready() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    blobStorage.put("foo!", new BlobRepository.BlobContent<BlobRepository>("foo!", blobData));
    BlobRepository.BlobContentRef<ByteBuffer> ref = repository.getBlobIncRef("foo!");
    assertEquals("", blobKey);
    assertFalse(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    assertNotNull("Key was not mapped to a BlobContent instance.", blobStorage.get("foo!"));
  }

  @Test
  public void testGetBlobIncrRefStringDecoder() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    BlobRepository.BlobContentRef<Object> ref =
        repository.getBlobIncRef(
            "foo!",
            new BlobRepository.Decoder<>() {
              @Override
              public String[][] decode(InputStream inputStream) {
                StringWriter writer = new StringWriter();
                try {
                  new InputStreamReader(inputStream, UTF8).transferTo(writer);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }

                assertEquals(BLOBSTR, writer.toString());
                return PARSED;
              }

              @Override
              public String getName() {
                return "mocked";
              }
            });
    assertEquals("foo!", blobKey);
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(PARSED, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    assertNotNull(blobStorage.get("foo!mocked"));
  }
}
