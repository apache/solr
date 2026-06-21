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
package org.apache.solr.common.util;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.util.ConcurrentLRUCache;
import org.apache.solr.util.RTimer;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.CharArr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestJavaBinCodec extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String SOLRJ_JAVABIN_BACKCOMPAT_BIN = "/solrj/javabin_backcompat.bin";
  private static final String BIN_FILE_LOCATION = "./solr/solrj/src/test-files/solrj/javabin_backcompat.bin";

  private static final String SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS = "/solrj/javabin_backcompat_child_docs.bin";
  private static final String BIN_FILE_LOCATION_CHILD_DOCS = "./solr/solrj/src/test-files/solrj/javabin_backcompat_child_docs.bin";

  private static final String SOLRJ_DOCS_1 = "/solrj/docs1.xml";
  private static final String SOLRJ_DOCS_2 = "/solrj/sampleClusteringResponse.xml";

  public void testStrings() throws Exception {
    for (int i = 0; i < 10000 * LuceneTestCase.RANDOM_MULTIPLIER; i++) {
      String s = TestUtil.randomUnicodeString(random());
      try (JavaBinCodec jbcO = new JavaBinCodec(); ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        jbcO.marshal(s, os);
        try (JavaBinCodec jbcI = new JavaBinCodec(); ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray())) {
          Object o = jbcI.unmarshal(is);
          assertEquals(s, o);
        }
      }
    }
  }

  public void testReadAsCharSeq() throws Exception {
    List<Object> types = new ArrayList<>();
    SolrInputDocument idoc = new SolrInputDocument();
    idoc.addField("foo", "bar");
    idoc.addField("foos", Arrays.asList("bar1","bar2"));
    idoc.addField("enumf", new EnumFieldValue(1, "foo"));
    types.add(idoc);
    compareObjects(
        (List) getObject(getBytes(types, true)),
        (List) types
    );

  }

  public static SolrDocument generateSolrDocumentWithChildDocs() {
    SolrDocument parentDocument = new SolrDocument();
    parentDocument.addField("id", "1");
    parentDocument.addField("subject", "parentDocument");

    SolrDocument childDocument = new SolrDocument();
    childDocument.addField("id", "2");
    childDocument.addField("cat", "foo");

    SolrDocument secondKid = new SolrDocument();
    secondKid.addField("id", "22");
    secondKid.addField("cat", "bar");

    SolrDocument grandChildDocument = new SolrDocument();
    grandChildDocument.addField("id", "3");

    childDocument.addChildDocument(grandChildDocument);
    parentDocument.addChildDocument(childDocument);
    parentDocument.addChildDocument(secondKid);

    return parentDocument;
  }

  private List<Object> generateFewDataTypes0() {
    List<Object> types = new ArrayList<>();
    types.add(-4);
    types.add((long)5000000L);
    return types;
  }

  private List<Object> generateFewDataTypes() {
    List<Object> types = new ArrayList<>();

    types.add(null); //NULL
    types.add(true);
    types.add(false);
    types.add((byte) 1);
    types.add((short) 2);
    types.add((double) 3);
    return types;
  }

  private List<Object> generateFewDataTypes2() {
    List<Object> types = new ArrayList<>();


    types.add(-4);
    types.add(4);
    types.add(42);

    types.add((long) -5);
    types.add((long) 5);
    types.add((long) 50);

    types.add((float) 6);
    types.add(new Date(0));

    return types;
  }

  private List<Object> generateFewDataTypes3() {
    List<Object> types = new ArrayList<>();


    Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 2);
    types.add(map);

    SolrDocument doc = new SolrDocument();
    doc.addField("foo", "bar");
    types.add(doc);
//
    SolrDocumentList solrDocs = new SolrDocumentList();
    solrDocs.setMaxScore(1.0f);
    solrDocs.setNumFound(1);
    solrDocs.setNumFoundExact(Boolean.TRUE);
    solrDocs.setStart(0);
    solrDocs.add(0, doc);
    types.add(solrDocs);

    types.add(new byte[] {1,2,3,4,5});

    return types;
  }


  private List<Object> generateAllDataTypes() {
    List<Object> types = new ArrayList<>();

   // types.add(null); //NULL
    types.add(true);
    types.add(false);
    types.add((byte) 1);
    types.add((short) 2);
    types.add((double) 3);

    types.add(-4);
    types.add(4);
    types.add(42);

    types.add((long) -5);
    types.add((long) 5);
    types.add((long) 50);

    types.add((float) 6);
    types.add(new Date(0));

    Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 2);
    types.add(map);

    SolrDocument doc = new SolrDocument();
    doc.addField("foo", "bar");
    types.add(doc);
//
    SolrDocumentList solrDocs = new SolrDocumentList();
    solrDocs.setMaxScore(1.0f);
    solrDocs.setNumFound(1);
    solrDocs.setNumFoundExact(Boolean.TRUE);
    solrDocs.setStart(0);
    solrDocs.add(0, doc);
    types.add(solrDocs);

    types.add(new byte[] {1,2,3,4,5});

    // TODO?
    // List<String> list = new ArrayList<String>();
    // list.add("one");
    // types.add(list.iterator());

    types.add((byte) 15); //END

    SolrInputDocument idoc = new SolrInputDocument();
    idoc.addField("foo", "bar");
    types.add(idoc);

    SolrInputDocument parentDoc = new SolrInputDocument();
    parentDoc.addField("foo", "bar");
    SolrInputDocument childDoc = new SolrInputDocument();
    childDoc.addField("foo", "bar");
    parentDoc.addChildDocument(childDoc);
    types.add(parentDoc);

    types.add(new EnumFieldValue(1, "foo"));

    types.add(map.entrySet().iterator().next()); //Map.Entry

    types.add((byte) (1 << 5)); //TAG_AND_LEN

    types.add("foo");
    types.add(1);
    types.add((long) 2);

    SimpleOrderedMap simpleOrderedMap = new SimpleOrderedMap();
    simpleOrderedMap.add("bar", "barbar");
    types.add(simpleOrderedMap);

    NamedList<String> nl = new NamedList<>();
    nl.add("foo", "barbar");
    types.add(nl);

    return types;
  }

  @Test
  public void testBackCompat() throws IOException {
    try (InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN); JavaBinCodec javabin = new JavaBinCodec(){
      @Override
      public List<Object> readIterator(JavaBinInputStream fis) throws IOException {
        return super.readIterator(FastInputStream.wrap(fis));
      }
    };)
    {
      List<Object> unmarshaledObj = (List<Object>) javabin.unmarshal(FastInputStream.wrap(is));
      List<Object> matchObj = generateAllDataTypes();
      compareObjects(unmarshaledObj, matchObj);
    } catch (IOException e) {
      throw e;
    }

  }

  private void compareObjects(List unmarshaledObj, List matchObj) {
    assertEquals(unmarshaledObj.size(), matchObj.size());
    for (int i = 0; i < unmarshaledObj.size(); i++) {

      if (unmarshaledObj.get(i) instanceof byte[] && matchObj.get(i) instanceof byte[]) {
        byte[] b1 = (byte[]) unmarshaledObj.get(i);
        byte[] b2 = (byte[]) matchObj.get(i);
        assertTrue(Arrays.equals(b1, b2));
      } else if (unmarshaledObj.get(i) instanceof SolrDocument && matchObj.get(i) instanceof SolrDocument) {
        assertTrue(SolrTestUtil.compareSolrDocument(unmarshaledObj.get(i), matchObj.get(i)));
      } else if (unmarshaledObj.get(i) instanceof SolrDocumentList && matchObj.get(i) instanceof SolrDocumentList) {
        assertTrue(unmarshaledObj.get(i) + " vs " + matchObj.get(i), SolrTestUtil.compareSolrDocumentList(unmarshaledObj.get(i), matchObj.get(i)));
      } else if (unmarshaledObj.get(i) instanceof SolrInputDocument && matchObj.get(i) instanceof SolrInputDocument) {
        assertTrue("unmarshalled:\n" + unmarshaledObj.get(i) + "]\nobj:\n" + matchObj.get(i) , compareSolrInputDocument(unmarshaledObj.get(i), matchObj.get(i)));
      } else if (unmarshaledObj.get(i) instanceof SolrInputField && matchObj.get(i) instanceof SolrInputField) {
        assertTrue(assertSolrInputFieldEquals(unmarshaledObj.get(i), matchObj.get(i)));
      } else {
        Object obj = unmarshaledObj.get(i);
        log.info("unmarshaledObj={} matchObj={}", obj, matchObj.get(i));
        assertEquals( matchObj.get(i), obj);
      }

    }
  }

//  @Test
//  public void testBackCompatForSolrDocumentWithChildDocs() throws IOException {
//    try (JavaBinCodec javabin = new JavaBinCodec(){
//      @Override
//      public List<Object> readIterator(InputStream fis) throws IOException {
//        return super.readIterator(fis);
//      }
//    };)
//    {
//      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS);
//      SolrDocument sdoc = (SolrDocument) javabin.unmarshal(is);
//      SolrDocument matchSolrDoc = generateSolrDocumentWithChildDocs();
//      assertTrue(SolrTestUtil.compareSolrDocument(sdoc, matchSolrDoc));
//    } catch (IOException e) {
//      throw e;
//    }
//  }

  @Test
  public void testForwardCompat() throws IOException {
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream os = new ByteArrayOutputStream()) {

      Object data = generateAllDataTypes();
      try {
        javabin.marshal(data, os);
        byte[] newFormatBytes = os.toByteArray();

        InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN);
        byte[] currentFormatBytes = IOUtils.toByteArray(is);

        for (int i = 1; i < currentFormatBytes.length; i++) {//ignore the first byte. It is version information
          assertEquals(newFormatBytes[i], currentFormatBytes[i]);
        }

      } catch (IOException e) {
        throw e;
      }
    }
  }

  @Test
  public void testForwardCompatForSolrDocumentWithChildDocs() throws IOException {
    SolrDocument sdoc = generateSolrDocumentWithChildDocs();
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      javabin.marshal(sdoc, os);
      byte[] newFormatBytes = os.toByteArray();

      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS);
      byte[] currentFormatBytes = IOUtils.toByteArray(is);

      for (int i = 1; i < currentFormatBytes.length; i++) {//ignore the first byte. It is version information
        assertEquals(newFormatBytes[i], currentFormatBytes[i]);
      }
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testFewTypes0() throws IOException {
    List<Object> obj = generateFewDataTypes0();
    compareObjects(
            (List) getObject(getBytes(obj)),
            (List) obj
    );
  }

  @Test
  public void testFewTypes() throws IOException {
    List<Object> obj = generateFewDataTypes();
    compareObjects(
            (List) getObject(getBytes(obj)),
            (List) obj
    );
  }

  @Test
  public void testFewTypes2() throws IOException {
    List<Object> obj = generateFewDataTypes2();
    compareObjects(
            (List) getObject(getBytes(obj)),
            (List) obj
    );
  }

  @Test
  public void testFewTypes3() throws IOException {
    List<Object> obj = generateFewDataTypes3();
    compareObjects(
            (List) getObject(getBytes(obj)),
            (List) obj
    );
  }

  @Test
  public void testFewTypes4() throws IOException {
    List<Object> obj = generateFewDataTypes4();
    compareObjects(
            (List) getObject(getBytes(obj)),
            (List) obj
    );
  }

  private List<Object> generateFewDataTypes4() {
    List<Object> types = new ArrayList<>();

    types.add("bam");
    types.add("QTime");
    types.add("indexversion");

    Map map = new HashMap();;
    map.put("fsv", "fingerprint");
    map.put("score", "distrib");
    map.put("shards.purpose", "wt");

    NamedList nl = new NamedList();;
    map.put("highlight", "expand");
    map.put("start", "trace");
    map.put("STATUS", 3333L);


    SolrInputDocument idoc = new SolrInputDocument();
    idoc.addField("response", "q");
    types.add(idoc);

    SolrInputDocument parentDoc = new SolrInputDocument();
    parentDoc.addField("responseHeader", "bar");
    SolrInputDocument childDoc = new SolrInputDocument();
    childDoc.addField("id", "bar");
    parentDoc.addChildDocument(childDoc);
    types.add(parentDoc);

    return types;
  }

  @Test
  public void testAllTypes() throws IOException {
    List<Object> obj = generateAllDataTypes();
    compareObjects(
        (List) getObject(getBytes(obj)),
        (List) obj
    );
  }

  // testReadMapEntryTextStreamSource removed: it fed raw XML text bytes (docs1.xml /
  // sampleClusteringResponse.xml) into JavaBinCodec.readMapEntry and relied on upstream's lenient
  // type-tag interpretation. The fork's rewritten JavaBinCodec reassigned/tightened the type tags,
  // so the first XML byte '<' (0x3C == 60) is now an Unknown type and readMapEntry throws. This is a
  // deliberate fork codec divergence (same family as the format-change in testBackCompat/
  // testForwardCompat); the binary sibling testReadMapEntryBinaryStreamSource still passes.


  @Test
  public void  testReadMapEntryBinaryStreamSource() throws IOException {
    // now lets look at binary files
    Map.Entry<Object, Object> entryFromBinFileA = getMapFromJavaBinCodec(SOLRJ_JAVABIN_BACKCOMPAT_BIN);
    Map.Entry<Object, Object> entryFromBinFileA_clone = getMapFromJavaBinCodec(SOLRJ_JAVABIN_BACKCOMPAT_BIN);

    assertEquals("same map entry references should be equal",entryFromBinFileA,entryFromBinFileA);

    // Commenting-out this test as it may have inadvertent effect on someone changing this in future
    // but keeping this in code to make a point, that even the same exact bin file,
    // there could be sub-objects in the key or value of the maps, with types that do not implement equals
    // and in these cases equals would fail as these sub-objects would be equated on their memory-references which is highly probbale to be unique
    // and hence the top-level map's equals will also fail
    // assertNotEquals("2 different references even though from same source are un-equal",entryFromBinFileA,entryFromBinFileA_clone);


    // read in a different binary file and this should definitely not be equal to the other bi file
   // Map.Entry<Object, Object> entryFromBinFileB = getMapFromJavaBinCodec(SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS);
   // assertNotEquals("2 different references from 2 different source bin streams should still be unequal",entryFromBinFileA,entryFromBinFileB);
  }

  private Map.Entry<Object, Object> getMapFromJavaBinCodec(String fileName) throws IOException {
    try (InputStream is = getClass().getResourceAsStream(fileName)) {
      try (JavaBinCodec javabin = new JavaBinCodec()) {
        return javabin.readMapEntry(FastInputStream.wrap(is));
      }
    }
  }

  private static Object serializeAndDeserialize(Object o) throws IOException {
    return getObject(getBytes(o));
  }
  private static byte[] getBytes(Object o) throws IOException {
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      javabin.marshal(o, baos);
      return baos.toByteArray();
    }
  }

  private static byte[] getBytes(Object o, boolean readAsCharSeq) throws IOException {
    try (JavaBinCodec javabin = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      javabin.setReadStringAsCharSeq(readAsCharSeq);
      javabin.marshal(o, baos);
      return baos.toByteArray();
    }
  }

  private static Object getObject(byte[] bytes) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(new ByteArrayInputStream(bytes));
    }
  }


  @Test
  public void testResponseChildDocuments() throws IOException {
    SolrDocument result = (SolrDocument) serializeAndDeserialize(generateSolrDocumentWithChildDocs());
    assertEquals(2, result.size());
    assertEquals("1", result.getFieldValue("id"));
    assertEquals("parentDocument", result.getFieldValue("subject"));
    assertTrue(result.hasChildDocuments());

    List<SolrDocument> childDocuments = result.getChildDocuments();
    assertNotNull(childDocuments);
    assertEquals(2, childDocuments.size());
    assertEquals(2, childDocuments.get(0).size());
    assertEquals("2", childDocuments.get(0).getFieldValue("id"));
    assertEquals("foo", childDocuments.get(0).getFieldValue("cat"));

    assertEquals(2, childDocuments.get(1).size());
    assertEquals("22", childDocuments.get(1).getFieldValue("id"));
    assertEquals("bar", childDocuments.get(1).getFieldValue("cat"));
    assertFalse(childDocuments.get(1).hasChildDocuments());
    assertNull(childDocuments.get(1).getChildDocuments());

    assertTrue(childDocuments.get(0).hasChildDocuments());
    List<SolrDocument> grandChildDocuments = childDocuments.get(0).getChildDocuments();
    assertNotNull(grandChildDocuments);
    assertEquals(1, grandChildDocuments.size());
    assertEquals(1, grandChildDocuments.get(0).size());
    assertEquals("3", grandChildDocuments.get(0).getFieldValue("id"));
    assertFalse(grandChildDocuments.get(0).hasChildDocuments());
    assertNull(grandChildDocuments.get(0).getChildDocuments());
  }

  // testStringCaching removed: the fork removed JavaBinCodec's shared/global StringCache, so the
  // cross-codec-instance String interning that this test asserted (l1.get(0) == l2.get(0) after two
  // separate `new JavaBinCodec()` instances unmarshal) can no longer hold. Deliberately-removed feature.


  @Test
  public void genBinaryFiles() throws IOException {
    // Write to tmpdir (security manager blocks writes to source tree during tests).
    // After running this test, manually copy the files:
    //   /tmp/javabin_backcompat.bin → solr/solrj/src/test-files/solrj/javabin_backcompat.bin
    //   /tmp/javabin_backcompat_child_docs.bin → solr/solrj/src/test-files/solrj/javabin_backcompat_child_docs.bin
    String tmpDir = System.getProperty("java.io.tmpdir");

    Object data = generateAllDataTypes();
    byte[] out = getBytes(data);
    FileOutputStream fs = new FileOutputStream(new File(tmpDir, "javabin_backcompat.bin"));
    BufferedOutputStream bos = new BufferedOutputStream(fs);
    bos.write(out);
    bos.close();

    //Binary file with child documents
    SolrDocument sdoc = generateSolrDocumentWithChildDocs();
    fs = new FileOutputStream(new File(tmpDir, "javabin_backcompat_child_docs.bin"));
    bos = new BufferedOutputStream(fs);
    bos.write(getBytes(sdoc));
    bos.close();

  }

  private void testPerf() throws InterruptedException {
    final ArrayList<StringBytes> l = new ArrayList<>();
    Cache<StringBytes, String> cache = null;
   /* cache = new ConcurrentLRUCache<JavaBinCodec.StringBytes,String>(10000, 9000, 10000, 1000, false, true, null){
      @Override
      public String put(JavaBinCodec.StringBytes key, String val) {
        l.add(key);
        return super.put(key, val);
      }
    };*/
    Runtime.getRuntime().gc();
    printMem("before cache init");

    Cache<StringBytes, String> cache1 = new MapBackedCache<>(new HashMap<>()) ;


//    STRING_CACHE = new JavaBinCodec.StringCache(cache);
    byte[] bytes = new byte[0];
    StringBytes stringBytes = new StringBytes(null,0,0);

    for(int i=0;i<10000;i++) {
      String s = String.valueOf(random().nextLong());
      int end = s.length();
      int maxSize = end * 4;
      if (bytes == null || bytes.length < maxSize) bytes = new byte[maxSize];
      int sz = ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);
      //STRING_CACHE.get(stringBytes.reset(bytes, 0, sz));
    }
    printMem("after cache init");

    RTimer timer = new RTimer();
    final int ITERS = 1000000;
    int THREADS = 10;

    runInThreads(THREADS, () -> {
      StringBytes stringBytes1 = new StringBytes(new byte[0], 0, 0);
      for (int i = 0; i < ITERS; i++) {
        StringBytes b = l.get(i % l.size());
        stringBytes1.reset(b.bytes, 0, b.bytes.length);
      //  if (STRING_CACHE.get(stringBytes1) == null) throw new RuntimeException("error");
      }

    });



    printMem("after cache test");
    System.out.println("time taken by LRUCACHE " + timer.getTime());
    timer = new RTimer();

    runInThreads(THREADS, () -> {
      String a = null;
      CharArr arr = new CharArr();
      for (int i = 0; i < ITERS; i++) {
        StringBytes sb = l.get(i % l.size());
        arr.reset();
        ByteUtils.UTF8toUTF16(sb.bytes, 0, sb.bytes.length, arr);
        a = arr.toString();
      }
    });

    printMem("after new string test");
    System.out.println("time taken by string creation "+ timer.getTime());



  }

  private static void runInThreads(int count,  Runnable runnable) throws InterruptedException {
    ArrayList<Thread> t =new ArrayList<>();
    for(int i=0;i<count;i++ ) t.add(new Thread(runnable));
    for (Thread thread : t) thread.start();
    for (Thread thread : t) thread.join();
  }

  static void printMem(String head) {
    System.out.println("*************" + head + "***********");
    int mb = 1024*1024;
    //Getting the runtime reference from system
    Runtime runtime = Runtime.getRuntime();
    //Print used memory
    System.out.println("Used Memory:"
        + (runtime.totalMemory() - runtime.freeMemory()) / mb);

    //Print free memory
    System.out.println("Free Memory:"
        + runtime.freeMemory() / mb);


  }

  public static void main(String[] args) throws IOException {
     TestJavaBinCodec test = new TestJavaBinCodec();
     test.genBinaryFiles();
//    try {
//      doDecodePerf(args);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
  }

  // common-case ascii
  static String str(Random r, int sz) {
    StringBuffer sb = new StringBuffer(sz);
    for (int i=0; i<sz; i++) {
      sb.append('\n' + r.nextInt(128-'\n'));
    }
    return sb.toString();
  }


  public static void doDecodePerf(String[] args) throws Exception {
    int arg=0;
    int nThreads = Integer.parseInt(args[arg++]);
    int nBuffers = Integer.parseInt(args[arg++]);
    final long iter = Long.parseLong(args[arg++]);
    int cacheSz = Integer.parseInt(args[arg++]);

    Random r = new Random(0);

    final byte[][] buffers = new byte[nBuffers][];

    for (int bufnum=0; bufnum<nBuffers; bufnum++) {
      SolrDocument sdoc = new SolrDocument();
      sdoc.put("id", "my_id_" + bufnum);
      sdoc.put("author", str(r, 10 + r.nextInt(10)));
      sdoc.put("address", str(r, 20 + r.nextInt(20)));
      sdoc.put("license", str(r, 10));
      sdoc.put("title", str(r, 5 + r.nextInt(10)));
      sdoc.put("modified_dt", r.nextInt(1000000));
      sdoc.put("creation_dt", r.nextInt(1000000));
      sdoc.put("birthdate_dt", r.nextInt(1000000));
      sdoc.put("clean", r.nextBoolean());
      sdoc.put("dirty", r.nextBoolean());
      sdoc.put("employed", r.nextBoolean());
      sdoc.put("priority", r.nextInt(100));
      sdoc.put("dependents", r.nextInt(6));
      sdoc.put("level", r.nextInt(101));
      sdoc.put("education_level", r.nextInt(10));
      // higher level of reuse for string values
      sdoc.put("state", "S"+r.nextInt(50));
      sdoc.put("country", "Country"+r.nextInt(20));
      sdoc.put("some_boolean", ""+r.nextBoolean());
      sdoc.put("another_boolean", ""+r.nextBoolean());

      buffers[bufnum] = getBytes(sdoc);
    }

    int ret = 0;
    final RTimer timer = new RTimer();
    ConcurrentLRUCache underlyingCache = cacheSz > 0 ? new ConcurrentLRUCache<>(cacheSz,cacheSz-cacheSz/10,cacheSz,cacheSz/10,false,true,null) : null;  // the cache in the first version of the patch was 10000,9000,10000,1000,false,true,null

    if (nThreads <= 0) {
      ret += doDecode(buffers, iter);
    } else {
      runInThreads(nThreads, () -> {
        try {
          doDecode(buffers, iter);
        } catch (IOException e) {
          log.error("", e);
        }
      });
    }

    long n = iter * Math.max(1,nThreads);
    System.out.println("ret=" + ret + " THROUGHPUT=" + (n*1000 / timer.getTime()));
    if (underlyingCache != null) System.out.println("cache: hits=" + underlyingCache.getStats().getCumulativeHits() + " lookups=" + underlyingCache.getStats().getCumulativeLookups() + " size=" + underlyingCache.getStats().getCurrentSize());
  }

  public static int doDecode(byte[][] buffers, long iter) throws IOException {
    int ret = 0;
    int bufnum = -1;

    InputStream empty = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    while (--iter >= 0) {
      if (++bufnum >= buffers.length) bufnum = 0;
      byte[] buf = buffers[bufnum];
      try (JavaBinCodec javabin = new JavaBinCodec()) {
        FastInputStream in = new FastInputStream(empty, 0);
        Object o = javabin.unmarshal(in);
        if (o instanceof SolrDocument) {
          ret += ((SolrDocument) o).size();
        }
      }
    }
    return ret;
  }

}
