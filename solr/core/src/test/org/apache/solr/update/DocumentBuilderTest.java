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
package org.apache.solr.update;

import static org.hamcrest.core.Is.is;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** */
public class DocumentBuilderTest extends SolrTestCaseJ4 {
  static final int save_min_len = DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST = save_min_len;
  }

  @After
  public void afterTest() {
    DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST = save_min_len;
  }

  @Test
  public void testBuildDocument() {
    SolrCore core = h.getCore();
    // undefined field
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("unknown field", 12345);

    SolrException ex =
        expectThrows(
            SolrException.class, () -> DocumentBuilder.toDocument(doc, core.getLatestSchema()));
    assertEquals("should be bad request", 400, ex.code());
  }

  @Test
  public void testNullField() {
    SolrCore core = h.getCore();

    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("name", null);
    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertNull(out.get("name"));
  }

  @Test
  public void testExceptions() {
    SolrCore core = h.getCore();

    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "123");
    doc.addField("unknown", "something");
    Exception ex =
        expectThrows(
            Exception.class, () -> DocumentBuilder.toDocument(doc, core.getLatestSchema()));
    assertTrue("should have document ID", ex.getMessage().indexOf("doc=123") > 0);
    doc.remove("unknown");

    doc.addField("weight", "not a number");
    ex =
        expectThrows(
            Exception.class, () -> DocumentBuilder.toDocument(doc, core.getLatestSchema()));
    assertTrue("should have document ID", ex.getMessage().indexOf("doc=123") > 0);
    assertTrue("cause is number format", ex.getCause() instanceof NumberFormatException);

    // now make sure it is OK
    doc.setField("weight", "1.34");
    DocumentBuilder.toDocument(doc, core.getLatestSchema());
  }

  @Test
  public void testMultiField() {
    SolrCore core = h.getCore();

    // make sure a null value is not indexed
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("home", "2.2,3.3");
    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertNotNull(out.get("home")); // contains the stored value and term vector, if there is one
    assertNotNull(out.getField("home_0" + FieldType.POLY_FIELD_SEPARATOR + "double"));
    assertNotNull(out.getField("home_1" + FieldType.POLY_FIELD_SEPARATOR + "double"));
  }

  /** Even though boosts have been removed, we still support them for bw compat. */
  public void testBoost() throws Exception {
    XmlDoc xml = new XmlDoc();
    xml.xml =
        "<doc>"
            + "<field name=\"id\">0</field>"
            + "<field name=\"title\" boost=\"3.0\">mytitle</field>"
            + "</doc>";
    assertNull(h.validateUpdate(add(xml, new String[0])));
  }

  /** It's ok to supply a document boost even if a field omits norms */
  public void testDocumentBoostOmitNorms() throws Exception {
    XmlDoc xml = new XmlDoc();
    xml.xml =
        "<doc boost=\"3.0\">"
            + "<field name=\"id\">2</field>"
            + "<field name=\"title_stringNoNorms\">mytitle</field>"
            + "</doc>";
    assertNull(h.validateUpdate(add(xml, new String[0])));
  }

  public void testSolrDocumentEquals() {

    String randomString = TestUtil.randomSimpleString(random());

    SolrDocument doc1 = new SolrDocument();
    doc1.addField("foo", randomString);

    SolrDocument doc2 = new SolrDocument();
    doc2.addField("foo", randomString);

    assertTrue(compareSolrDocument(doc1, doc2));

    doc1.addField("foo", "bar");

    assertFalse(compareSolrDocument(doc1, doc2));

    doc1 = new SolrDocument();
    doc1.addField("bar", randomString);

    assertFalse(compareSolrDocument(doc1, doc2));

    int randomInt = random().nextInt();
    doc1 = new SolrDocument();
    doc1.addField("foo", randomInt);
    doc2 = new SolrDocument();
    doc2.addField("foo", randomInt);

    assertTrue(compareSolrDocument(doc1, doc2));

    doc2 = new SolrDocument();
    doc2.addField("bar", randomInt);

    assertFalse(compareSolrDocument(doc1, doc2));
  }

  public void testSolrInputDocumentEquality() {

    String randomString = TestUtil.randomSimpleString(random());

    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("foo", randomString);
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("foo", randomString);

    assertTrue(compareSolrInputDocument(doc1, doc2));

    doc1 = new SolrInputDocument();
    doc1.addField("foo", randomString);
    doc2 = new SolrInputDocument();
    doc2.addField("foo", randomString);

    SolrInputDocument childDoc = new SolrInputDocument();
    childDoc.addField("foo", "bar");

    doc1.addChildDocument(childDoc);
    assertFalse(compareSolrInputDocument(doc1, doc2));

    doc2.addChildDocument(childDoc);
    assertTrue(compareSolrInputDocument(doc1, doc2));

    SolrInputDocument childDoc1 = new SolrInputDocument();
    childDoc.addField(TestUtil.randomSimpleString(random()), TestUtil.randomSimpleString(random()));
    doc2.addChildDocument(childDoc1);
    assertFalse(compareSolrInputDocument(doc1, doc2));
  }

  public void testSolrInputFieldEquality() {
    String randomString = TestUtil.randomSimpleString(random(), 10, 20);

    int val = random().nextInt();
    SolrInputField sif1 = new SolrInputField(randomString);
    sif1.setValue(val);
    SolrInputField sif2 = new SolrInputField(randomString);
    sif2.setValue(val);

    assertTrue(assertSolrInputFieldEquals(sif1, sif2));

    sif2.setName("foo");
    assertFalse(assertSolrInputFieldEquals(sif1, sif2));
  }

  public void testMoveLargestLast() {
    SolrInputDocument inDoc = new SolrInputDocument();
    // not stored.  It won't be moved.  This value is the longest, however.
    String TEXT_FLD = "text";
    inDoc.addField(
        TEXT_FLD,
        "NOT STORED|"
            + RandomStrings.randomAsciiOfLength(
                random(), 4 * DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST));

    String CAT_FLD = "cat"; // stored, multiValued
    inDoc.addField(CAT_FLD, "STORED V1|");
    //  pretty long value
    inDoc.addField(
        CAT_FLD,
        "STORED V2|"
            + RandomStrings.randomAsciiOfLength(
                random(), 2 * DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST));
    inDoc.addField(
        CAT_FLD,
        "STORED V3|"
            + RandomStrings.randomAsciiOfLength(random(), DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST));

    String SUBJECT_FLD = "subject"; // stored.  This value is long, but not long enough.
    inDoc.addField(
        SUBJECT_FLD,
        "2ndplace|"
            + RandomStrings.randomAsciiOfLength(random(), DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST));

    Document outDoc = DocumentBuilder.toDocument(inDoc, h.getCore().getLatestSchema());

    // filter outDoc by stored fields; convert to list.
    List<IndexableField> storedFields =
        StreamSupport.stream(outDoc.spliterator(), false)
            .filter(f -> f.fieldType().stored())
            .collect(Collectors.toList());
    // clip to last 3.  We expect these to be for CAT_FLD
    storedFields = storedFields.subList(storedFields.size() - 3, storedFields.size());

    Iterator<IndexableField> fieldIterator = storedFields.iterator();
    IndexableField field;

    // Test that we retained the particular value ordering, even though the 2nd of three was
    // longest

    assertTrue(fieldIterator.hasNext());
    field = fieldIterator.next();
    assertEquals(CAT_FLD, field.name());
    assertTrue(field.stringValue().startsWith("STORED V1|"));

    assertTrue(fieldIterator.hasNext());
    field = fieldIterator.next();
    assertEquals(CAT_FLD, field.name());
    assertTrue(field.stringValue().startsWith("STORED V2|"));

    assertTrue(fieldIterator.hasNext());
    field = fieldIterator.next();
    assertEquals(CAT_FLD, field.name());
    assertTrue(field.stringValue().startsWith("STORED V3|"));
  }

  @Test
  public void testCopyFieldMaxChars() {
    SolrCore core = h.getCore();

    String testValue = "this is more than 10 characters";
    String truncatedValue = "this is mo";

    // maxChars with a string value
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("title", testValue);

    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertEquals(testValue, out.get("title"));
    assertEquals(truncatedValue, out.get("max_chars"));

    // maxChars with a ByteArrayUtf8CharSequence
    doc = new SolrInputDocument();
    doc.addField("title", new ByteArrayUtf8CharSequence(testValue));

    out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertEquals(testValue, out.get("title"));
    assertEquals(truncatedValue, out.get("max_chars"));
  }

  @Test
  public void testMultipleCopyFieldMaxChars() {
    SolrCore core = h.getCore();

    String testValue = "this is more than 10 characters";
    String truncatedValue = "this is mo";

    // maxChars with a string value
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("title", testValue);

    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertEquals(testValue, out.get("title"));
    assertEquals(truncatedValue, out.get("max_chars"));
    assertEquals(testValue, out.get("no_max_chars"));
    assertEquals(testValue, out.get("large_max_chars"));

    // maxChars with a ByteArrayUtf8CharSequence
    doc = new SolrInputDocument();
    doc.addField("title", new ByteArrayUtf8CharSequence(testValue));

    out = DocumentBuilder.toDocument(doc, core.getLatestSchema());
    assertEquals(testValue, out.get("title"));
    assertEquals(truncatedValue, out.get("max_chars"));
    assertEquals(testValue, out.get("no_max_chars"));
    assertEquals(testValue, out.get("large_max_chars"));
  }

  @Test
  public void denseVector_shouldReturnOneIndexableFieldAndOneStoredFieldPerVectorElement() {
    SolrCore core = h.getCore();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "0");
    doc.addField("vector", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));

    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());

    // from /solr/core/src/test-files/solr/collection1/conf/schema.xml
    KnnVectorField expectedIndexableField =
        new KnnVectorField(
            "vector", new float[] {1.1f, 2.1f, 3.1f, 4.1f}, VectorSimilarityFunction.COSINE);

    MatcherAssert.assertThat(
        ((KnnFloatVectorField) out.getField("vector")).vectorValue(),
        is(expectedIndexableField.vectorValue()));

    List<IndexableField> storedFields =
        StreamSupport.stream(out.spliterator(), false)
            .filter(f -> (f.fieldType().stored() && f.name().equals("vector")))
            .collect(Collectors.toList());
    MatcherAssert.assertThat(storedFields.size(), is(4));

    MatcherAssert.assertThat(storedFields.get(0).numericValue(), is(1.1f));
    MatcherAssert.assertThat(storedFields.get(1).numericValue(), is(2.1f));
    MatcherAssert.assertThat(storedFields.get(2).numericValue(), is(3.1f));
    MatcherAssert.assertThat(storedFields.get(3).numericValue(), is(4.1f));
  }

  @Test
  public void denseVector_shouldWorkWithCopyFields() {
    SolrCore core = h.getCore();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "0");
    doc.addField("vector", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));

    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());

    // from /solr/core/src/test-files/solr/collection1/conf/schema.xml
    KnnVectorField expectedDestination =
        new KnnVectorField(
            "vector2", new float[] {1.1f, 2.1f, 3.1f, 4.1f}, VectorSimilarityFunction.DOT_PRODUCT);

    MatcherAssert.assertThat(
        ((KnnFloatVectorField) out.getField("vector2")).vectorValue(),
        is(expectedDestination.vectorValue()));

    List<IndexableField> storedFields =
        StreamSupport.stream(out.spliterator(), false)
            .filter(f -> (f.fieldType().stored() && f.name().equals("vector2")))
            .collect(Collectors.toList());
    MatcherAssert.assertThat(storedFields.size(), is(4));

    MatcherAssert.assertThat(storedFields.get(0).numericValue(), is(1.1f));
    MatcherAssert.assertThat(storedFields.get(1).numericValue(), is(2.1f));
    MatcherAssert.assertThat(storedFields.get(2).numericValue(), is(3.1f));
    MatcherAssert.assertThat(storedFields.get(3).numericValue(), is(4.1f));
  }

  @Test
  public void denseVector_incorrectCopyFieldDestinationType_shouldThrowException() {
    SolrCore core = h.getCore();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "0");
    doc.addField("vector3", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));

    RuntimeException thrown =
        assertThrows(
            "Incorrect destination field type should raise exception",
            SolrException.class,
            () -> {
              DocumentBuilder.toDocument(doc, core.getLatestSchema());
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "ERROR: [doc=0] Error adding field 'vector3'='[1.1, 2.1, 3.1, 4.1]' msg=The copy field destination must be a DenseVectorField: vector_f_p"));
  }

  @Test
  public void denseVector_incorrectCopyFieldDestinationDimension_shouldThrowException() {
    SolrCore core = h.getCore();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "0");
    doc.addField("vector4", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));

    RuntimeException thrown =
        assertThrows(
            "Incorrect destination dimension should raise exception",
            SolrException.class,
            () -> {
              DocumentBuilder.toDocument(doc, core.getLatestSchema());
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "ERROR: [doc=0] Error adding field 'vector4'='[1.1, 2.1, 3.1, 4.1]' msg=Error while creating field 'vector5{type=knn_vector5,properties=indexed,stored}' from value '[1.1, 2.1, 3.1, 4.1]'"));
    MatcherAssert.assertThat(
        thrown.getCause().getCause().getMessage(),
        is(
            "incorrect vector dimension. The vector value has size 4 while it is expected a vector with size 5"));
  }
}
