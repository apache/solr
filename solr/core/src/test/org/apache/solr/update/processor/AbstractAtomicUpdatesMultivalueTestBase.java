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
package org.apache.solr.update.processor;

import static org.apache.solr.SolrTestCaseJ4.sdoc;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.EmbeddedSolrServerTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer.RequestWriterSupplier;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractAtomicUpdatesMultivalueTestBase extends EmbeddedSolrServerTestBase {

  protected static void initWithRequestWriter(RequestWriterSupplier requestWriterSupplier)
      throws Exception {
    solrClientTestRule.startSolr(Paths.get(SolrTestCaseJ4.TEST_HOME()));

    System.setProperty("enable.update.log", "true");
    SolrTestCaseJ4.newRandomConfig();
    System.setProperty("solr.test.sys.prop1", "propone"); // TODO yuck; remove
    System.setProperty("solr.test.sys.prop2", "proptwo"); // TODO yuck; remove

    solrClientTestRule.newCollection().withConfigSet("../collection1").create();
  }

  @Before
  public void before() throws SolrServerException, IOException {
    getSolrClient().deleteByQuery("*:*");
  }

  private void assertQR(final String fieldName, final String queryValue, final int numFound)
      throws SolrServerException, IOException {

    SolrQuery query = new SolrQuery("q", fieldName + ":" + queryValue);
    QueryResponse rsp = getSolrClient().query(query);
    assertEquals(numFound, rsp.getResults().getNumFound());
  }

  private void runTestForField(
      final String fieldName,
      final Object[] values,
      final String[] queries,
      final Optional<Function<Object, Object>> valueConverter)
      throws SolrServerException, IOException {

    final Function<Object, Object> vc = valueConverter.orElse(o -> o);

    getSolrClient()
        .add(
            Arrays.asList(
                sdoc("id", "20000", fieldName, Arrays.asList(values[0], values[1], values[2])),
                sdoc("id", "20001", fieldName, Arrays.asList(values[1], values[2], values[3]))));
    getSolrClient().commit(true, true);

    if (queries != null) {
      assertQR(fieldName, queries[0], 1);
      assertQR(fieldName, queries[1], 2);
      assertQR(fieldName, queries[2], 2);
      assertQR(fieldName, queries[3], 1);
    }

    Collection<Object> fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(3, fieldValues.size());
    MatcherAssert.assertThat(
        fieldValues, hasItems(vc.apply(values[0]), vc.apply(values[1]), vc.apply(values[2])));
    MatcherAssert.assertThat(fieldValues, not(hasItems(vc.apply(values[3]))));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(3, fieldValues.size());
    MatcherAssert.assertThat(
        fieldValues, hasItems(vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3])));
    MatcherAssert.assertThat(fieldValues, not(hasItems(vc.apply(values[0]))));

    getSolrClient().add(sdoc("id", "20000", fieldName, Map.of("remove", List.of(values[0]))));
    getSolrClient().commit(true, true);

    if (queries != null) {
      assertQR(fieldName, queries[0], 0);
      assertQR(fieldName, queries[1], 2);
      assertQR(fieldName, queries[2], 2);
      assertQR(fieldName, queries[3], 1);
    }

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(vc.apply(values[1]), vc.apply(values[2])));
    MatcherAssert.assertThat(fieldValues, not(hasItems(vc.apply(values[0]), vc.apply(values[3]))));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(3, fieldValues.size());
    MatcherAssert.assertThat(
        fieldValues, hasItems(vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3])));
    MatcherAssert.assertThat(fieldValues, not(hasItems(vc.apply(values[0]))));

    getSolrClient()
        .add(
            sdoc(
                "id",
                "20001",
                fieldName,
                Map.of("remove", List.of(values[0], values[1], values[2]))));
    getSolrClient().commit(true, true);

    if (queries != null) {
      assertQR(fieldName, queries[0], 0);
      assertQR(fieldName, queries[1], 1);
      assertQR(fieldName, queries[2], 1);
      assertQR(fieldName, queries[3], 1);
    }

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(vc.apply(values[1]), vc.apply(values[2])));
    MatcherAssert.assertThat(fieldValues, not(hasItems(vc.apply(values[0]), vc.apply(values[3]))));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(1, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(vc.apply(values[3])));
    MatcherAssert.assertThat(
        fieldValues, not(hasItems(vc.apply(values[0]), vc.apply(values[1]), vc.apply(values[2]))));

    getSolrClient()
        .add(
            Arrays.asList(
                sdoc(
                    "id",
                    "20000",
                    fieldName,
                    Map.of("add", List.of(values[0]), "remove", List.of(values[1], values[2]))),
                sdoc(
                    "id",
                    "20001",
                    fieldName,
                    Map.of("add", List.of(values[0]), "remove", List.of(values[3])))));
    getSolrClient().commit(true, true);

    if (queries != null) {
      assertQR(fieldName, queries[0], 2);
      assertQR(fieldName, queries[1], 0);
      assertQR(fieldName, queries[2], 0);
      assertQR(fieldName, queries[3], 0);
    }

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(1, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(vc.apply(values[0])));
    MatcherAssert.assertThat(
        fieldValues, not(hasItems(vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3]))));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(1, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(vc.apply(values[0])));
    MatcherAssert.assertThat(
        fieldValues, not(hasItems(vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3]))));

    getSolrClient()
        .add(
            Arrays.asList(
                sdoc(
                    "id",
                    "20000",
                    fieldName,
                    Map.of("set", List.of(values[0], values[1], values[2], values[3]))),
                sdoc(
                    "id",
                    "20001",
                    fieldName,
                    Map.of("set", List.of(values[0], values[1], values[2], values[3])))));
    getSolrClient().commit(true, true);

    if (queries != null) {
      assertQR(fieldName, queries[0], 2);
      assertQR(fieldName, queries[1], 2);
      assertQR(fieldName, queries[2], 2);
      assertQR(fieldName, queries[3], 2);
    }

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(4, fieldValues.size());
    MatcherAssert.assertThat(
        fieldValues,
        hasItems(
            vc.apply(values[0]), vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3])));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(4, fieldValues.size());
    MatcherAssert.assertThat(
        fieldValues,
        hasItems(
            vc.apply(values[0]), vc.apply(values[1]), vc.apply(values[2]), vc.apply(values[3])));
  }

  private String[] toStringArray(final Object[] values) {
    return Arrays.stream(values)
        .map(v -> v.toString())
        .collect(Collectors.toList())
        .toArray(new String[] {});
  }

  private void runTestForFieldWithQuery(final String fieldName, final Object[] values)
      throws SolrServerException, IOException {
    runTestForField(fieldName, values, toStringArray(values), Optional.empty());
  }

  private void runTestForFieldWithQuery(
      final String fieldName, final Object[] values, final String[] queries)
      throws SolrServerException, IOException {
    runTestForField(fieldName, values, queries, Optional.empty());
  }

  private void runTestForFieldWithQuery(
      final String fieldName,
      final Object[] values,
      final String[] queries,
      final Function<Object, Object> valueConverter)
      throws SolrServerException, IOException {
    runTestForField(fieldName, values, queries, Optional.of(valueConverter));
  }

  private void runTestForFieldWithoutQuery(final String fieldName, final Object[] values)
      throws SolrServerException, IOException {
    runTestForField(fieldName, values, null, Optional.empty());
  }

  @Test
  public void testMultivalueBinaryField() throws SolrServerException, IOException {
    runTestForFieldWithoutQuery(
        "binaryRemove",
        new byte[][] {new byte[] {0}, new byte[] {1}, new byte[] {2}, new byte[] {3}});
  }

  @Test
  public void testMultivalueBooleanField() throws SolrServerException, IOException {

    final String fieldName = "booleanRemove";

    getSolrClient()
        .add(
            Arrays.asList(
                sdoc("id", "20000", fieldName, List.of(true, false)),
                sdoc("id", "20001", fieldName, List.of(false, true))));
    getSolrClient().commit(true, true);

    assertQR(fieldName, "true", 2);
    assertQR(fieldName, "false", 2);

    Collection<Object> fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));

    getSolrClient().add(sdoc("id", "20000", fieldName, Map.of("remove", List.of(false))));
    getSolrClient().commit(true, true);

    assertQR(fieldName, "true", 2);
    assertQR(fieldName, "false", 1);

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(1, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));

    getSolrClient().add(sdoc("id", "20001", fieldName, Map.of("remove", List.of(true, false))));
    getSolrClient().commit(true, true);

    assertQR(fieldName, "true", 1);
    assertQR(fieldName, "false", 0);

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(1, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true));
    MatcherAssert.assertThat(fieldValues, not(hasItems(false)));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertNull(fieldValues);

    getSolrClient()
        .add(Arrays.asList(sdoc("id", "20000", fieldName, Map.of("add", List.of(false, false)))));
    getSolrClient().commit(true, true);

    assertQR(fieldName, "true", 1);
    assertQR(fieldName, "false", 1);

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(3, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertNull(fieldValues);

    getSolrClient()
        .add(
            Arrays.asList(
                sdoc("id", "20000", fieldName, Map.of("set", List.of(true, false))),
                sdoc("id", "20001", fieldName, Map.of("set", List.of(false, true)))));
    getSolrClient().commit(true, true);

    assertQR(fieldName, "true", 2);
    assertQR(fieldName, "false", 2);

    fieldValues = getSolrClient().getById("20000").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));
    fieldValues = getSolrClient().getById("20001").getFieldValues(fieldName);
    assertEquals(2, fieldValues.size());
    MatcherAssert.assertThat(fieldValues, hasItems(true, false));
  }

  @Test
  public void testMultivalueCollationField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("collationRemove", new String[] {"cf1", "cf2", "cf3", "cf4"});
  }

  @Test
  public void testMultivalueDatePointField() throws SolrServerException, IOException {

    final String s1 = "1980-01-01T00:00:00Z";
    final Date d1 = Date.from(ZonedDateTime.parse(s1).toInstant());
    final String s2 = "1990-01-01T00:00:00Z";
    final Date d2 = Date.from(ZonedDateTime.parse(s2).toInstant());
    final String s3 = "2000-01-01T00:00:00Z";
    final Date d3 = Date.from(ZonedDateTime.parse(s3).toInstant());
    final String s4 = "2010-01-01T00:00:00Z";
    final Date d4 = Date.from(ZonedDateTime.parse(s4).toInstant());

    runTestForFieldWithQuery(
        "datePointRemove",
        new Date[] {d1, d2, d3, d4},
        new String[] {"\"" + s1 + "\"", "\"" + s2 + "\"", "\"" + s3 + "\"", "\"" + s4 + "\""});
  }

  @Test
  public void testMultivalueDateRangeField() throws SolrServerException, IOException {

    final String s1 = "1980-01-01T00:00:00Z";
    final String s2 = "1990-01-01T00:00:00Z";
    final String s3 = "2000-01-01T00:00:00Z";
    final String s4 = "2010-01-01T00:00:00Z";

    runTestForFieldWithQuery(
        "dateRangeRemove",
        new String[] {s1, s2, s3, s4},
        new String[] {"\"" + s1 + "\"", "\"" + s2 + "\"", "\"" + s3 + "\"", "\"" + s4 + "\""});
  }

  @Test
  public void testMultivalueDoublePointField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("doublePointRemove", new Double[] {1.0d, 2.0d, 3.0d, 4.0d});
  }

  @Test
  public void testMultivalueEnumField() throws SolrServerException, IOException {
    runTestForFieldWithQuery(
        "enumRemove_sev_enum", new Object[] {"Low", "Medium", "High", "Critical"});
  }

  @Test
  public void testMultivalueEnumFieldWithNumbers() throws SolrServerException, IOException {
    final Object[] values = new Object[] {"Low", "Medium", "High", 11};
    runTestForFieldWithQuery(
        "enumRemove_sev_enum",
        values,
        toStringArray(values),
        o -> {
          if (Integer.valueOf(11).equals(o)) {
            return "Critical";
          } else {
            return o;
          }
        });
  }

  @Test
  public void testMultivalueExternalFileField() throws SolrServerException, IOException {
    runTestForFieldWithoutQuery(
        "externalFileRemove", new String[] {"file1.txt", "file2.txt", "file3.txt", "file4.txt"});
  }

  @Test
  public void testMultivalueFloatPointField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("floatPointRemove", new Float[] {1.0f, 2.0f, 3.0f, 4.0f});
  }

  @Test
  public void testMultivalueICUCollationField() throws SolrServerException, IOException {
    runTestForFieldWithQuery(
        "icuCollationRemove", new String[] {"iuccf1", "icucf2", "icucf3", "icucf4"});
  }

  @Test
  public void testMultivalueIntPointField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("intPointRemove", new Integer[] {1, 2, 3, 4});
  }

  @Test
  public void testMultivalueLatLonPointSpatialField() throws SolrServerException, IOException {
    runTestForFieldWithoutQuery(
        "latLonPointSpatialRemove", new String[] {"1.0,-1.0", "2.0,-2.0", "3.0,-3.0", "4.0,-4.0"});
  }

  @Test
  public void testMultivalueLatLonField() throws SolrServerException, IOException {
    String[] values = {"1.0,-1.0", "2.0,-2.0", "3.0,-3.0", "4.0,-4.0"};
    String[] queries =
        Arrays.stream(values)
            .map(
                v ->
                    v.substring(v.indexOf(',') + 1)
                        + " "
                        + v.substring(0, v.indexOf(','))) // map "lat,lon" to "lon lat"
            .map(v -> "\"Intersects(BUFFER(POINT(" + v + "),0.001))\"")
            .toArray(String[]::new);
    runTestForFieldWithQuery("latLonRemove", values, queries);
  }

  @Test
  public void testMultivalueLongPointField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("longPointRemove", new Long[] {1l, 2l, 3l, 4l});
  }

  @Test
  public void testMultivaluePointField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("pointRemove", new String[] {"1,1", "2,2", "3,3", "4,4"});
  }

  @Test
  public void testMultivalueRandomSortField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("randomSortRemove", new String[] {"rsf1", "rsf2", "rsf3", "rsf4"});
  }

  @Test
  public void testMultivalueSpatialRecursivePrefixTreeFieldType()
      throws SolrServerException, IOException {
    runTestForFieldWithoutQuery(
        "spatialRecursivePrefixTreeRemove", new String[] {"1,1", "2,2", "3,3", "4,4"});
  }

  @Test
  public void testMultivalueStringField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("stringRemove", new String[] {"str1", "str2", "str3", "str4"});
  }

  @Test
  public void testMultivalueStringFieldUsingCharSequence() throws SolrServerException, IOException {
    final ByteArrayUtf8CharSequence[] values =
        new ByteArrayUtf8CharSequence[] {
          new ByteArrayUtf8CharSequence("str1"),
          new ByteArrayUtf8CharSequence("str2"),
          new ByteArrayUtf8CharSequence("str3"),
          new ByteArrayUtf8CharSequence("str4")
        };
    runTestForFieldWithQuery("stringRemove", values, toStringArray(values), o -> o.toString());
  }

  @Test
  public void testMultivalueTextField() throws SolrServerException, IOException {
    runTestForFieldWithQuery("textRemove", new String[] {"text1", "text2", "text3", "text4"});
  }

  @Test
  public void testMultivalueUUIDField() throws SolrServerException, IOException {
    final String[] values =
        new String[] {
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString()
        };
    runTestForFieldWithQuery("uuidRemove", values);
  }
}
