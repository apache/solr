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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.ErrorLogMuter;
import org.junit.BeforeClass;

/**
 * Tests the basics of configuring FieldMutatingUpdateProcessors (mainly via
 * TrimFieldUpdateProcessor) and the logic of other various subclasses.
 */
public class FieldMutatingUpdateProcessorTest extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }

  public void testComprehensive() throws Exception {

    final String countMe = "how long is this string?";
    final int count = countMe.length();

    processAdd(
        "comprehensive",
        doc(
            f("id", "1111"),
            f("primary_author_s1", "XXXX", "Adam", "Sam"),
            f("all_authors_s1", "XXXX", "Adam", "Sam"),
            f("foo_is", countMe, 42),
            f("first_foo_l", countMe, -34),
            f("max_foo_l", countMe, -34),
            f("min_foo_l", countMe, -34)));

    assertU(commit());

    assertQ(
        req("id:1111"),
        "//str[@name='primary_author_s1'][.='XXXX']",
        "//str[@name='all_authors_s1'][.='XXXX; Adam; Sam']",
        "//arr[@name='foo_is']/int[1][.='" + count + "']",
        "//arr[@name='foo_is']/int[2][.='42']",
        "//long[@name='max_foo_l'][.='" + count + "']",
        "//long[@name='first_foo_l'][.='" + count + "']",
        "//long[@name='min_foo_l'][.='-34']");
  }

  @SuppressWarnings("UnnecessaryStringBuilder")
  public void testTrimAll() throws Exception {
    SolrInputDocument d = null;

    d =
        processAdd(
            "trim-all",
            doc(
                f("id", "1111"),
                f("name", " Hoss ", new StringBuilder(" Man")),
                f("foo_t", " some text ", "other Text\t"),
                f("foo_d", 42),
                field("foo_s", " string ")));

    assertNotNull(d);

    // simple stuff
    assertEquals("string", d.getFieldValue("foo_s"));
    assertEquals(List.of("some text", "other Text"), List.copyOf(d.getFieldValues("foo_t")));
    assertEquals(List.of("Hoss", "Man"), List.copyOf(d.getFieldValues("name")));

    // slightly more interesting
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));
  }

  public void testTrimAllRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("child_s", " grandchild ");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("child_s", " child ");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("root_s", " root ");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("trim-all", root);

    assertNotNull(d);
    assertEquals("root", root.getFieldValue("root_s"));
    assertEquals("child", child.getFieldValue("child_s"));
    assertEquals("grandchild", grandChild.getFieldValue("child_s"));
  }

  public void testTrimFieldsRecurseIntoNestedDocumentsOnlyWhenParentSelected() throws Exception {
    // The "trim-fields" chain selects only name and foo_t. A child document given
    // as the value of the unselected child_doc field is left untouched, even for
    // fields the selector names, while a child document under the selected foo_t
    // field is mutated like any other selected field's value.
    final SolrInputDocument childUnderUnselected = new SolrInputDocument();
    childUnderUnselected.addField("name", " unselected child ");
    childUnderUnselected.addField("foo_t", " unselected text ");

    final SolrInputDocument childUnderSelected = new SolrInputDocument();
    childUnderSelected.addField("name", " selected child ");

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("name", " root ");
    root.addField("child_doc", childUnderUnselected);
    root.addField("foo_t", childUnderSelected);

    final SolrInputDocument d = processAdd("trim-fields", root);

    assertNotNull(d);
    assertEquals("root", root.getFieldValue("name"));
    assertEquals(" unselected child ", childUnderUnselected.getFieldValue("name"));
    assertEquals(" unselected text ", childUnderUnselected.getFieldValue("foo_t"));
    assertEquals("selected child", childUnderSelected.getFieldValue("name"));
  }

  public void testUniqValues() throws Exception {
    final String chain = "uniq-values";
    SolrInputDocument d = null;
    d =
        processAdd(
            chain,
            doc(
                f("id", "1111"),
                f("name", "Hoss", "Man", "Hoss"),
                f("uniq_1_s", "Hoss", "Man", "Hoss"),
                f("uniq_2_s", "Foo", "Hoss", "Man", "Hoss", "Bar"),
                f("uniq_3_s", 5.0F, 23, "string", 5.0F)));

    assertNotNull(d);

    assertEquals(List.of("Hoss", "Man", "Hoss"), List.copyOf(d.getFieldValues("name")));
    assertEquals(List.of("Hoss", "Man"), List.copyOf(d.getFieldValues("uniq_1_s")));
    assertEquals(List.of("Foo", "Hoss", "Man", "Bar"), List.copyOf(d.getFieldValues("uniq_2_s")));
    assertEquals(List.of(5.0F, 23, "string"), List.copyOf(d.getFieldValues("uniq_3_s")));
  }

  public void testTrimFields() throws Exception {
    for (String chain : List.of("trim-fields", "trim-fields-arr")) {
      SolrInputDocument d = null;
      d =
          processAdd(
              chain,
              doc(
                  f("id", "1111"),
                  f("name", " Hoss ", " Man"),
                  f("foo_t", " some text ", "other Text\t"),
                  f("foo_s", " string ")));

      assertNotNull(d);

      assertEquals(" string ", d.getFieldValue("foo_s"));
      assertEquals(List.of("some text", "other Text"), List.copyOf(d.getFieldValues("foo_t")));
      assertEquals(List.of("Hoss", "Man"), List.copyOf(d.getFieldValues("name")));
    }
  }

  public void testTrimField() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-field",
            doc(
                f("id", "1111"),
                f("name", " Hoss ", " Man"),
                f("foo_t", " some text ", "other Text\t"),
                f("foo_s", " string ")));

    assertNotNull(d);

    assertEquals(" string ", d.getFieldValue("foo_s"));
    assertEquals(List.of("some text", "other Text"), List.copyOf(d.getFieldValues("foo_t")));
    assertEquals(List.of(" Hoss ", " Man"), List.copyOf(d.getFieldValues("name")));
  }

  public void testTrimRegex() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-field-regexes",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foozat_s", " string2 "),
                f("bar_t", " string3 "),
                f("bar_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foozat_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_t"));
    assertEquals("string4", d.getFieldValue("bar_s"));
  }

  public void testTrimTypes() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-types",
            doc(
                f("id", "1111"),
                f("foo_sw", " string0 "),
                f("name", " string1 "),
                f("title", " string2 "),
                f("bar_t", " string3 "),
                f("bar_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string0", d.getFieldValue("foo_sw"));
    assertEquals("string1", d.getFieldValue("name"));
    assertEquals("string2", d.getFieldValue("title"));
    assertEquals(" string3 ", d.getFieldValue("bar_t"));
    assertEquals(" string4 ", d.getFieldValue("bar_s"));
  }

  public void testTrimClasses() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-classes",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
  }

  public void testTrimMultipleRules() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-multi",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
  }

  public void testTrimExclusions() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "trim-most",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 ")));

    assertNotNull(d);

    assertEquals(" string1 ", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));

    d =
        processAdd(
            "trim-many",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 "),
                f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));

    d =
        processAdd(
            "trim-few",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 "),
                f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals(" string3 ", d.getFieldValue("bar_dt"));
    assertEquals(" string4 ", d.getFieldValue("bar_HOSS_s"));

    d =
        processAdd(
            "trim-some",
            doc(
                f("id", "1111"),
                f("foo_t", " string1 "),
                f("foo_s", " string2 "),
                f("bar_dt", " string3 "),
                f("bar_HOSS_s", " string4 ")));

    assertNotNull(d);

    assertEquals("string1", d.getFieldValue("foo_t"));
    assertEquals("string2", d.getFieldValue("foo_s"));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("string4", d.getFieldValue("bar_HOSS_s"));
  }

  public void testRemoveBlanks() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "remove-all-blanks",
            doc(
                f("id", "1111"),
                f("foo_s", "string1", ""),
                f("bar_dt", "string2", "", "string3"),
                f("yak_t", ""),
                f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(List.of("string1"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals(List.of("string2", "string3"), List.copyOf(d.getFieldValues("bar_dt")));
    assertFalse("shouldn't be any values for yak_t", d.containsKey("yak_t"));
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));
  }

  public void testRemoveBlanksRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grandchild");
    grandChild.addField("yak_t", "");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("foo_s", "");
    child.addField("yak_t", "child");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "1111");
    root.addField("foo_s", "root");
    root.addField("yak_t", "");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("remove-all-blanks", root);

    assertNotNull(d);
    assertEquals(List.of("root"), List.copyOf(root.getFieldValues("foo_s")));
    assertFalse(root.containsKey("yak_t"));
    assertFalse(child.containsKey("foo_s"));
    assertEquals(List.of("child"), List.copyOf(child.getFieldValues("yak_t")));
    assertEquals(List.of("grandchild"), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertFalse(grandChild.containsKey("yak_t"));
  }

  public void testStrLength() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "length-none",
            doc(
                f("id", "1111"),
                f("foo_s", "string1", "string222"),
                f("bar_dt", "string3"),
                f("yak_t", ""),
                f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(List.of("string1", "string222"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals("", d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));

    d =
        processAdd(
            "length-some",
            doc(
                f("id", "1111"),
                f("foo_s", "string1", "string222"),
                f("bar_dt", "string3"),
                f("yak_t", ""),
                f("foo_d", 42)));

    assertNotNull(d);

    assertEquals(List.of(7, 9), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals("string3", d.getFieldValue("bar_dt"));
    assertEquals(0, d.getFieldValue("yak_t"));
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));
  }

  public void testRegexReplace() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "regex-replace",
            doc(
                f("id", "doc1"),
                f("content", "This is         a text\t with a lot\n     of whitespace"),
                f("title", "This\ttitle     has a lot of    spaces")));

    assertNotNull(d);

    assertEquals("ThisXisXaXtextXwithXaXlotXofXwhitespace", d.getFieldValue("content"));
    assertEquals("ThisXtitleXhasXaXlotXofXspaces", d.getFieldValue("title"));

    // literalReplacement = true
    d =
        processAdd(
            "regex-replace-literal-true",
            doc(
                f("id", "doc2"),
                f("content", "Let's try this one"),
                f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one", d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one", d.getFieldValue("title"));

    // literalReplacement is not specified, defaults to true
    d =
        processAdd(
            "regex-replace-literal-default-true",
            doc(
                f("id", "doc3"),
                f("content", "Let's try this one"),
                f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one", d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one", d.getFieldValue("title"));

    // if user passes literalReplacement as a string param instead of boolean
    d =
        processAdd(
            "regex-replace-literal-str-true",
            doc(
                f("id", "doc4"),
                f("content", "Let's try this one"),
                f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <$1> this one", d.getFieldValue("content"));
    assertEquals("Let's <$1> <$1> this one", d.getFieldValue("title"));

    // This is with literalReplacement = false
    d =
        processAdd(
            "regex-replace-literal-false",
            doc(
                f("id", "doc5"),
                f("content", "Let's try this one"),
                f("title", "Let's try try this one")));

    assertNotNull(d);

    assertEquals("Let's <try> this one", d.getFieldValue("content"));
    assertEquals("Let's <try> <try> this one", d.getFieldValue("title"));
  }

  public void testRegexReplaceRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("content", "grand child text");
    grandChild.addField("title", "grand child title");
    grandChild.addField("other_s", "grand child other");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("content", "child text");
    child.addField("title", "child title");
    child.addField("other_s", "child other");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "regex-root");
    root.addField("content", "root text");
    root.addField("title", "root title");
    root.addField("other_s", "root other");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("regex-replace", root);

    assertNotNull(d);
    assertEquals("rootXtext", root.getFieldValue("content"));
    assertEquals("rootXtitle", root.getFieldValue("title"));
    assertEquals("root other", root.getFieldValue("other_s"));
    assertEquals("childXtext", child.getFieldValue("content"));
    assertEquals("childXtitle", child.getFieldValue("title"));
    assertEquals("child other", child.getFieldValue("other_s"));
    assertEquals("grandXchildXtext", grandChild.getFieldValue("content"));
    assertEquals("grandXchildXtitle", grandChild.getFieldValue("title"));
    assertEquals("grand child other", grandChild.getFieldValue("other_s"));
  }

  public void testFirstValue() throws Exception {
    SolrInputDocument d = null;

    d =
        processAdd(
            "first-value",
            doc(
                f("id", "1111"),
                f("foo_s", "string1", "string222"),
                f("bar_s", "string3"),
                f("yak_t", "string4", "string5")));

    assertNotNull(d);

    assertEquals(List.of("string1"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals(List.of("string3"), List.copyOf(d.getFieldValues("bar_s")));
    assertEquals(List.of("string4", "string5"), List.copyOf(d.getFieldValues("yak_t")));
  }

  public void testLastValue() throws Exception {
    SolrInputDocument d = null;

    // basics

    d =
        processAdd(
            "last-value",
            doc(
                f("id", "1111"),
                f("foo_s", "string1", "string222"),
                f("bar_s", "string3"),
                f("yak_t", "string4", "string5")));

    assertNotNull(d);

    assertEquals(List.of("string222"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals(List.of("string3"), List.copyOf(d.getFieldValues("bar_s")));
    assertEquals(List.of("string4", "string5"), List.copyOf(d.getFieldValues("yak_t")));

    // test optimizations (and force test of defaults)

    SolrInputField special = null;

    // test something that's definitely a SortedSet

    special = new SolrInputField("foo_s");
    special.setValue(new TreeSet<>(List.of("ggg", "first", "last", "hhh")));

    d = processAdd("last-value", doc(f("id", "1111"), special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that's definitely a List

    special = new SolrInputField("foo_s");
    special.setValue(List.of("first", "ggg", "hhh", "last"));

    d = processAdd("last-value", doc(f("id", "1111"), special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));

    // test something that is definitely not a List or SortedSet
    // (ie: get default behavior of Collection using iterator)

    special = new SolrInputField("foo_s");
    special.setValue(new LinkedHashSet<>(List.of("first", "ggg", "hhh", "last")));

    d = processAdd("last-value", doc(f("id", "1111"), special));

    assertNotNull(d);

    assertEquals("last", d.getFieldValue("foo_s"));
  }

  @SuppressWarnings("try")
  public void testMinValue() throws Exception {
    SolrInputDocument d = null;

    d =
        processAdd(
            "min-value",
            doc(
                f("id", "1111"),
                f("foo_s", "zzz", "aaa", "bbb"),
                f("foo_i", 42, 128, -3),
                f("bar_s", "aaa"),
                f("yak_t", "aaa", "bbb")));

    assertNotNull(d);

    assertEquals(List.of("aaa"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals(List.of(-3), List.copyOf(d.getFieldValues("foo_i")));
    assertEquals(List.of("aaa"), List.copyOf(d.getFieldValues("bar_s")));
    assertEquals(List.of("aaa", "bbb"), List.copyOf(d.getFieldValues("yak_t")));

    // failure when un-comparable

    SolrException error = null;
    try (ErrorLogMuter ignored = ErrorLogMuter.regex(".*Unable to mutate field.*")) {
      d =
          processAdd(
              "min-value",
              doc(
                  f("id", "1111"),
                  f("foo_s", "zzz", 42, "bbb"),
                  f("bar_s", "aaa"),
                  f("yak_t", "aaa", "bbb")));
    } catch (SolrException e) {
      error = e;
    }
    assertNotNull("no error on un-comparable values", error);
    assertTrue("error doesn't mention field name", error.getMessage().contains("foo_s"));
  }

  @SuppressWarnings("try")
  public void testMaxValue() throws Exception {
    SolrInputDocument d = null;

    d =
        processAdd(
            "max-value",
            doc(
                f("id", "1111"),
                f("foo_s", "zzz", "aaa", "bbb"),
                f("foo_i", 42, 128, -3),
                f("bar_s", "aaa"),
                f("yak_t", "aaa", "bbb")));

    assertNotNull(d);

    assertEquals(List.of("zzz"), List.copyOf(d.getFieldValues("foo_s")));
    assertEquals(List.of(128), List.copyOf(d.getFieldValues("foo_i")));
    assertEquals(List.of("aaa"), List.copyOf(d.getFieldValues("bar_s")));
    assertEquals(List.of("aaa", "bbb"), List.copyOf(d.getFieldValues("yak_t")));

    // failure when un-comparable

    SolrException error = null;
    try (ErrorLogMuter ignored = ErrorLogMuter.regex(".*Unable to mutate field.*")) {
      d =
          processAdd(
              "min-value",
              doc(
                  f("id", "1111"),
                  f("foo_s", "zzz", 42, "bbb"),
                  f("bar_s", "aaa"),
                  f("yak_t", "aaa", "bbb")));
    } catch (SolrException e) {
      error = e;
    }
    assertNotNull("no error on un-comparable values", error);
    assertTrue("error doesn't mention field name", error.getMessage().contains("foo_s"));
  }

  public void testSubsetValueProcessorsRecurseIntoNestedDocuments() throws Exception {
    SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grand first");
    grandChild.addField("foo_s", "grand second");
    grandChild.addField("foo_i", 9);
    grandChild.addField("foo_i", 3);
    grandChild.addField("foo_i", 7);
    grandChild.addField("bar_s", "grand bar first");
    grandChild.addField("bar_s", "grand bar second");
    grandChild.addField("other_s", "grand untouched");

    SolrInputDocument child = new SolrInputDocument();
    child.addField("foo_s", "child first");
    child.addField("foo_s", "child second");
    child.addField("foo_i", 6);
    child.addField("foo_i", 2);
    child.addField("foo_i", 8);
    child.addField("bar_s", "child bar first");
    child.addField("bar_s", "child bar second");
    child.addField("other_s", "child untouched");
    child.addChildDocument(grandChild);

    SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "subset-root");
    root.addField("foo_s", "root first");
    root.addField("foo_s", "root second");
    root.addField("foo_i", 4);
    root.addField("foo_i", 1);
    root.addField("foo_i", 5);
    root.addField("bar_s", "root bar first");
    root.addField("bar_s", "root bar second");
    root.addField("other_s", "root untouched");
    root.addChildDocument(child);

    SolrInputDocument d = processAdd("first-value", root);
    assertNotNull(d);
    assertEquals(List.of("root first"), List.copyOf(root.getFieldValues("foo_s")));
    assertEquals(List.of("child first"), List.copyOf(child.getFieldValues("foo_s")));
    assertEquals(List.of("grand first"), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertEquals(List.of("root bar first"), List.copyOf(root.getFieldValues("bar_s")));
    assertEquals(List.of("child bar first"), List.copyOf(child.getFieldValues("bar_s")));
    assertEquals(List.of("grand bar first"), List.copyOf(grandChild.getFieldValues("bar_s")));
    assertEquals(List.of("root untouched"), List.copyOf(root.getFieldValues("other_s")));
    assertEquals(List.of("child untouched"), List.copyOf(child.getFieldValues("other_s")));
    assertEquals(List.of("grand untouched"), List.copyOf(grandChild.getFieldValues("other_s")));

    grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grand first");
    grandChild.addField("foo_s", "grand second");
    grandChild.addField("foo_i", 9);
    grandChild.addField("foo_i", 3);
    grandChild.addField("foo_i", 7);
    grandChild.addField("bar_s", "grand bar first");
    grandChild.addField("bar_s", "grand bar second");
    grandChild.addField("other_s", "grand untouched");

    child = new SolrInputDocument();
    child.addField("foo_s", "child first");
    child.addField("foo_s", "child second");
    child.addField("foo_i", 6);
    child.addField("foo_i", 2);
    child.addField("foo_i", 8);
    child.addField("bar_s", "child bar first");
    child.addField("bar_s", "child bar second");
    child.addField("other_s", "child untouched");
    child.addChildDocument(grandChild);

    root = new SolrInputDocument();
    root.addField("id", "subset-root");
    root.addField("foo_s", "root first");
    root.addField("foo_s", "root second");
    root.addField("foo_i", 4);
    root.addField("foo_i", 1);
    root.addField("foo_i", 5);
    root.addField("bar_s", "root bar first");
    root.addField("bar_s", "root bar second");
    root.addField("other_s", "root untouched");
    root.addChildDocument(child);

    d = processAdd("last-value", root);
    assertNotNull(d);
    assertEquals(List.of("root second"), List.copyOf(root.getFieldValues("foo_s")));
    assertEquals(List.of("child second"), List.copyOf(child.getFieldValues("foo_s")));
    assertEquals(List.of("grand second"), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertEquals(List.of("root bar second"), List.copyOf(root.getFieldValues("bar_s")));
    assertEquals(List.of("child bar second"), List.copyOf(child.getFieldValues("bar_s")));
    assertEquals(List.of("grand bar second"), List.copyOf(grandChild.getFieldValues("bar_s")));

    grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grand zzz");
    grandChild.addField("foo_s", "grand aaa");
    grandChild.addField("foo_s", "grand mmm");
    grandChild.addField("foo_i", 9);
    grandChild.addField("foo_i", 3);
    grandChild.addField("foo_i", 7);
    grandChild.addField("bar_s", "grand zzz");
    grandChild.addField("bar_s", "grand aaa");
    grandChild.addField("bar_s", "grand mmm");

    child = new SolrInputDocument();
    child.addField("foo_s", "child zzz");
    child.addField("foo_s", "child aaa");
    child.addField("foo_s", "child mmm");
    child.addField("foo_i", 6);
    child.addField("foo_i", 2);
    child.addField("foo_i", 8);
    child.addField("bar_s", "child zzz");
    child.addField("bar_s", "child aaa");
    child.addField("bar_s", "child mmm");
    child.addChildDocument(grandChild);

    root = new SolrInputDocument();
    root.addField("id", "subset-root");
    root.addField("foo_s", "root zzz");
    root.addField("foo_s", "root aaa");
    root.addField("foo_s", "root mmm");
    root.addField("foo_i", 4);
    root.addField("foo_i", 1);
    root.addField("foo_i", 5);
    root.addField("bar_s", "root zzz");
    root.addField("bar_s", "root aaa");
    root.addField("bar_s", "root mmm");
    root.addChildDocument(child);

    d = processAdd("min-value", root);
    assertNotNull(d);
    assertEquals(List.of("root aaa"), List.copyOf(root.getFieldValues("foo_s")));
    assertEquals(List.of("child aaa"), List.copyOf(child.getFieldValues("foo_s")));
    assertEquals(List.of("grand aaa"), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertEquals(List.of(1), List.copyOf(root.getFieldValues("foo_i")));
    assertEquals(List.of(2), List.copyOf(child.getFieldValues("foo_i")));
    assertEquals(List.of(3), List.copyOf(grandChild.getFieldValues("foo_i")));
    assertEquals(List.of("root aaa"), List.copyOf(root.getFieldValues("bar_s")));
    assertEquals(List.of("child aaa"), List.copyOf(child.getFieldValues("bar_s")));
    assertEquals(List.of("grand aaa"), List.copyOf(grandChild.getFieldValues("bar_s")));

    grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grand zzz");
    grandChild.addField("foo_s", "grand aaa");
    grandChild.addField("foo_s", "grand mmm");
    grandChild.addField("foo_i", 9);
    grandChild.addField("foo_i", 3);
    grandChild.addField("foo_i", 7);
    grandChild.addField("bar_s", "grand zzz");
    grandChild.addField("bar_s", "grand aaa");
    grandChild.addField("bar_s", "grand mmm");

    child = new SolrInputDocument();
    child.addField("foo_s", "child zzz");
    child.addField("foo_s", "child aaa");
    child.addField("foo_s", "child mmm");
    child.addField("foo_i", 6);
    child.addField("foo_i", 2);
    child.addField("foo_i", 8);
    child.addField("bar_s", "child zzz");
    child.addField("bar_s", "child aaa");
    child.addField("bar_s", "child mmm");
    child.addChildDocument(grandChild);

    root = new SolrInputDocument();
    root.addField("id", "subset-root");
    root.addField("foo_s", "root zzz");
    root.addField("foo_s", "root aaa");
    root.addField("foo_s", "root mmm");
    root.addField("foo_i", 4);
    root.addField("foo_i", 1);
    root.addField("foo_i", 5);
    root.addField("bar_s", "root zzz");
    root.addField("bar_s", "root aaa");
    root.addField("bar_s", "root mmm");
    root.addChildDocument(child);

    d = processAdd("max-value", root);
    assertNotNull(d);
    assertEquals(List.of("root zzz"), List.copyOf(root.getFieldValues("foo_s")));
    assertEquals(List.of("child zzz"), List.copyOf(child.getFieldValues("foo_s")));
    assertEquals(List.of("grand zzz"), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertEquals(List.of(5), List.copyOf(root.getFieldValues("foo_i")));
    assertEquals(List.of(8), List.copyOf(child.getFieldValues("foo_i")));
    assertEquals(List.of(9), List.copyOf(grandChild.getFieldValues("foo_i")));
    assertEquals(List.of("root zzz"), List.copyOf(root.getFieldValues("bar_s")));
    assertEquals(List.of("child zzz"), List.copyOf(child.getFieldValues("bar_s")));
    assertEquals(List.of("grand zzz"), List.copyOf(grandChild.getFieldValues("bar_s")));
  }

  public void testHtmlStrip() throws Exception {
    SolrInputDocument d = null;

    d =
        processAdd(
            "html-strip",
            doc(
                f("id", "1111"),
                f("html_s", "<body>hi &amp; bye", "aaa", "bbb"),
                f("bar_s", "<body>hi &amp; bye")));

    assertNotNull(d);

    assertEquals(List.of("hi & bye", "aaa", "bbb"), List.copyOf(d.getFieldValues("html_s")));
    assertEquals("<body>hi &amp; bye", d.getFieldValue("bar_s"));
  }

  public void testHtmlStripRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("html_s", "<i>grand</i> child");
    grandChild.addField("bar_s", "<i>grand</i> untouched");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("html_s", "<p>child</p>");
    child.addField("bar_s", "<p>child untouched</p>");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "html-root");
    root.addField("html_s", "<b>root</b>");
    root.addField("bar_s", "<b>root untouched</b>");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("html-strip", root);

    assertNotNull(d);
    assertEquals("root", root.getFieldValue("html_s"));
    assertEquals("<b>root untouched</b>", root.getFieldValue("bar_s"));
    assertEquals("child", child.getFieldValue("html_s"));
    assertEquals("<p>child untouched</p>", child.getFieldValue("bar_s"));
    assertEquals("grand child", grandChild.getFieldValue("html_s"));
    assertEquals("<i>grand</i> untouched", grandChild.getFieldValue("bar_s"));
  }

  public void testTruncate() throws Exception {
    SolrInputDocument d = null;

    d = processAdd("truncate", doc(f("id", "1111"), f("trunc", "123456789", "", 42, "abcd")));

    assertNotNull(d);

    assertEquals(List.of("12345", "", 42, "abcd"), List.copyOf(d.getFieldValues("trunc")));
  }

  public void testTruncateRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("trunc", "grandchild");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("trunc", "child");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "trunc-root");
    root.addField("trunc", "root-value");
    root.addField("other_s", "root-value");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("truncate", root);

    assertNotNull(d);
    assertEquals("root-", root.getFieldValue("trunc"));
    assertEquals("root-value", root.getFieldValue("other_s"));
    assertEquals("child", child.getFieldValue("trunc"));
    assertEquals("grand", grandChild.getFieldValue("trunc"));
  }

  public void testStrLengthRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grandchild");
    grandChild.addField("yak_t", "grand yak");
    grandChild.addField("bar_dt", "grand bar");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("foo_s", "child");
    child.addField("yak_t", "");
    child.addField("bar_dt", "child bar");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "length-root");
    root.addField("foo_s", "root");
    root.addField("yak_t", "yak");
    root.addField("bar_dt", "root bar");
    root.addField("foo_d", 42);
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("length-some", root);

    assertNotNull(d);
    assertEquals(List.of(4), List.copyOf(root.getFieldValues("foo_s")));
    assertEquals(List.of(3), List.copyOf(root.getFieldValues("yak_t")));
    assertEquals("root bar", root.getFieldValue("bar_dt"));
    assertEquals(42, root.getFieldValue("foo_d"));
    assertEquals(List.of(5), List.copyOf(child.getFieldValues("foo_s")));
    assertEquals(List.of(0), List.copyOf(child.getFieldValues("yak_t")));
    assertEquals("child bar", child.getFieldValue("bar_dt"));
    assertEquals(List.of(10), List.copyOf(grandChild.getFieldValues("foo_s")));
    assertEquals(List.of(9), List.copyOf(grandChild.getFieldValues("yak_t")));
    assertEquals("grand bar", grandChild.getFieldValue("bar_dt"));
  }

  public void testIgnore() throws Exception {

    IndexSchema schema = h.getCore().getLatestSchema();
    assertNull(
        "test expects 'foo_giberish' to not be a valid field, looks like schema was changed out from under us",
        schema.getFieldTypeNoEx("foo_giberish"));
    assertNull(
        "test expects 'bar_giberish' to not be a valid field, looks like schema was changed out from under us",
        schema.getFieldTypeNoEx("bar_giberish"));
    assertNotNull(
        "test expects 't_raw' to be a valid field, looks like schema was changed out from under us",
        schema.getFieldTypeNoEx("t_raw"));
    assertNotNull(
        "test expects 'foo_s' to be a valid field, looks like schema was changed out from under us",
        schema.getFieldTypeNoEx("foo_s"));

    SolrInputDocument d = null;

    d =
        processAdd(
            "ignore-not-in-schema",
            doc(
                f("id", "1111"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));

    assertNotNull(d);
    assertFalse(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("foo_giberish"));
    assertEquals(List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("t_raw")));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d =
        processAdd(
            "ignore-some",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));

    assertNotNull(d);
    assertEquals(
        List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("foo_giberish")));
    assertEquals(
        List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("bar_giberish")));
    assertFalse(d.containsKey("t_raw"));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d =
        processAdd(
            "ignore-not-in-schema-explicit-selector",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertFalse(d.containsKey("bar_giberish"));
    assertEquals(List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("t_raw")));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d =
        processAdd(
            "ignore-not-in-schema-and-foo-name-prefix",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertEquals(
        List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("bar_giberish")));
    assertEquals(List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("t_raw")));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d =
        processAdd(
            "ignore-foo-name-prefix-except-not-schema",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertEquals(
        List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("foo_giberish")));
    assertEquals(
        List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("bar_giberish")));
    assertEquals(List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("t_raw")));
    assertFalse(d.containsKey("foo_s"));

    d =
        processAdd(
            "ignore-in-schema",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertTrue(d.containsKey("foo_giberish"));
    assertTrue(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("id"));
    assertFalse(d.containsKey("t_raw"));
    assertFalse(d.containsKey("foo_s"));

    d =
        processAdd(
            "ignore-not-in-schema-explicit-str-selector",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertFalse(d.containsKey("foo_giberish"));
    assertFalse(d.containsKey("bar_giberish"));
    assertEquals(List.of("123456789", "", 42, "abcd"), List.copyOf(d.getFieldValues("t_raw")));
    assertEquals("hoss", d.getFieldValue("foo_s"));

    d =
        processAdd(
            "ignore-in-schema-str-selector",
            doc(
                f("id", "1111"),
                f("foo_giberish", "123456789", "", 42, "abcd"),
                f("bar_giberish", "123456789", "", 42, "abcd"),
                f("t_raw", "123456789", "", 42, "abcd"),
                f("foo_s", "hoss")));
    assertNotNull(d);
    assertTrue(d.containsKey("foo_giberish"));
    assertTrue(d.containsKey("bar_giberish"));
    assertFalse(d.containsKey("id"));
    assertFalse(d.containsKey("t_raw"));
    assertFalse(d.containsKey("foo_s"));
  }

  public void testIgnoreRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("grand_giberish", "ignore me");
    grandChild.addField("foo_s", "grandchild");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("child_giberish", "ignore me too");
    child.addField("foo_s", "child");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "1111");
    root.addField("foo_s", "root");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("ignore-not-in-schema", root);

    assertNotNull(d);
    assertEquals("root", root.getFieldValue("foo_s"));
    assertEquals("child", child.getFieldValue("foo_s"));
    assertEquals("grandchild", grandChild.getFieldValue("foo_s"));
    assertFalse(child.containsKey("child_giberish"));
    assertFalse(grandChild.containsKey("grand_giberish"));
  }

  public void testCountValues() throws Exception {

    SolrInputDocument d = null;

    // trivial
    d = processAdd("count", doc(f("id", "1111"), f("count_field", "aaa", "bbb", "ccc")));

    assertNotNull(d);
    assertEquals(3, d.getFieldValue("count_field"));

    // edge case: no values to count, means no count
    // (use default if you want one)
    d = processAdd("count", doc(f("id", "1111")));

    assertNotNull(d);
    assertFalse(d.containsKey("count_field"));

    // typical usecase: clone and count
    d =
        processAdd(
            "clone-then-count",
            doc(
                f("id", "1111"),
                f("category", "scifi", "war", "space"),
                f("editors", "John W. Campbell"),
                f("list_price", 1000)));
    assertNotNull(d);
    assertEquals(List.of("scifi", "war", "space"), List.copyOf(d.getFieldValues("category")));
    assertEquals(3, d.getFieldValue("category_count"));
    assertEquals(List.of("John W. Campbell"), List.copyOf(d.getFieldValues("editors")));
    assertEquals(1000, d.getFieldValue("list_price"));

    // typical use case: clone and count demonstrating default
    d =
        processAdd(
            "clone-then-count",
            doc(f("id", "1111"), f("editors", "Anonymous"), f("list_price", 1000)));
    assertNotNull(d);
    assertEquals(0, d.getFieldValue("category_count"));
    assertEquals(List.of("Anonymous"), List.copyOf(d.getFieldValues("editors")));
    assertEquals(1000, d.getFieldValue("list_price"));
  }

  public void testConcatDefaults() throws Exception {
    SolrInputDocument d = null;
    d =
        processAdd(
            "concat-defaults",
            doc(
                f("id", "1111", "222"),
                f("attr_foo", "string1", "string2"),
                f("foo_s1", "string3", "string4"),
                f("bar_dt", "string5", "string6"),
                f("bar_HOSS_s", "string7", "string8"),
                f("foo_d", 42)));

    assertNotNull(d);

    assertEquals("1111, 222", d.getFieldValue("id"));
    assertEquals(List.of("string1", "string2"), List.copyOf(d.getFieldValues("attr_foo")));
    assertEquals("string3, string4", d.getFieldValue("foo_s1"));
    assertEquals(List.of("string5", "string6"), List.copyOf(d.getFieldValues("bar_dt")));
    assertEquals(List.of("string7", "string8"), List.copyOf(d.getFieldValues("bar_HOSS_s")));
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));
  }

  public void testConcatExplicit() throws Exception {
    doSimpleDelimTest("concat-field", ", ");
  }

  public void testConcatExplicitWithDelim() throws Exception {
    doSimpleDelimTest("concat-type-delim", "; ");
  }

  public void testConcatFieldRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("foo_s", "grandchild-a");
    grandChild.addField("foo_s", "grandchild-b");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("foo_s", "child-a");
    child.addField("foo_s", "child-b");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("foo_s", "root-a");
    root.addField("foo_s", "root-b");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("concat-field", root);

    assertNotNull(d);
    assertEquals("root-a, root-b", root.getFieldValue("foo_s"));
    assertEquals("child-a, child-b", child.getFieldValue("foo_s"));
    assertEquals("grandchild-a, grandchild-b", grandChild.getFieldValue("foo_s"));
  }

  public void testCountRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("count_field", "grandchild-a");
    grandChild.addField("count_field", "grandchild-b");
    grandChild.addField("count_field", "grandchild-c");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("count_field", "child-a");
    child.addField("count_field", "child-b");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("id", "1111");
    root.addField("count_field", "root-a");
    root.addField("count_field", "root-b");
    root.addField("count_field", "root-c");
    root.addField("count_field", "root-d");
    root.addChildDocument(child);

    final SolrInputDocument d = processAdd("count", root);

    assertNotNull(d);
    assertEquals(4, root.getFieldValue("count_field"));
    assertEquals(2, child.getFieldValue("count_field"));
    assertEquals(3, grandChild.getFieldValue("count_field"));
  }

  public void testMutatingProcessorsRecurseIntoNestedDocuments() throws Exception {
    final SolrInputDocument grandChild = new SolrInputDocument();
    grandChild.addField("child_s", "grandchild");

    final SolrInputDocument child = new SolrInputDocument();
    child.addField("child_s", "child");
    child.addChildDocument(grandChild);

    final SolrInputDocument root = new SolrInputDocument();
    root.addField("root_s", "root");
    root.addChildDocument(child);

    final UpdateRequestProcessor noOpNext = new UpdateRequestProcessor(null) {};
    final FieldMutatingUpdateProcessor processor =
        new FieldMutatingUpdateProcessor(fname -> fname.endsWith("_s"), noOpNext) {
          @Override
          protected SolrInputField mutate(SolrInputField src) {
            final SolrInputField dest = new SolrInputField(src.getName());
            for (Object value : src.getValues()) {
              dest.addValue("mutated-" + value);
            }
            return dest;
          }
        };

    final AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = root;

    processor.processAdd(cmd);

    assertEquals("mutated-root", root.getFieldValue("root_s"));
    assertEquals("mutated-child", child.getFieldValue("child_s"));
    assertEquals("mutated-grandchild", grandChild.getFieldValue("child_s"));
  }

  private void doSimpleDelimTest(final String chain, final String delim) throws Exception {

    SolrInputDocument d = null;
    d =
        processAdd(
            chain,
            doc(
                f("id", "1111"),
                f("foo_t", "string1", "string2"),
                f("foo_d", 42),
                field("foo_s", "string3", "string4")));

    assertNotNull(d);

    assertEquals(List.of("string1", "string2"), List.copyOf(d.getFieldValues("foo_t")));
    assertEquals("string3" + delim + "string4", d.getFieldValue("foo_s"));

    // slightly more interesting
    assertEquals("processor borked non string value", 42, d.getFieldValue("foo_d"));
  }
}
