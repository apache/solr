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
package org.apache.solr.spelling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @since solr 1.3
 */
@SuppressTempFileChecks(
    bugUrl = "https://issues.apache.org/jira/browse/SOLR-1877 Spellcheck IndexReader leak bug?")
public class IndexBasedSpellCheckerTest extends SolrTestCaseJ4 {
  protected static SpellingQueryConverter queryConverter;

  protected static String[] DOCS =
      new String[] {
        "This is a title",
        "The quick reb fox jumped over the lazy brown dogs.",
        "This is a document",
        "another document",
        "red fox",
        "green bun",
        "green bud"
      };

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    // Index something with a title
    for (int i = 0; i < DOCS.length; i++) {
      assertNull(h.validateUpdate(adoc("id", String.valueOf(i), "title", DOCS[i])));
    }
    assertNull(h.validateUpdate(commit()));
    queryConverter = new SimpleQueryConverter();
  }

  @AfterClass
  public static void afterClass() {
    queryConverter = null;
  }

  /** Resets the stream and reads its first token, for tests that convert a single-word query. */
  private static SpellCheckToken firstToken(Supplier<TokenStream> supplier) throws IOException {
    TokenStream stream = supplier.get();
    stream.reset();
    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = stream.addAttribute(TypeAttribute.class);
    PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
    FlagsAttribute flagsAtt = stream.addAttribute(FlagsAttribute.class);
    PayloadAttribute payloadAtt = stream.addAttribute(PayloadAttribute.class);
    stream.incrementToken();
    SpellCheckToken token =
        new SpellCheckToken(
            termAtt.toString(),
            offsetAtt.startOffset(),
            offsetAtt.endOffset(),
            typeAtt.type(),
            posIncAtt.getPositionIncrement(),
            flagsAtt.getFlags(),
            payloadAtt.getPayload());
    while (stream.incrementToken()) {
      // drain any remaining tokens before end()
    }
    stream.end();
    stream.close();
    return token;
  }

  @Test
  public void testComparator() {
    SpellCheckComponent component =
        (SpellCheckComponent) h.getCore().getSearchComponent("spellcheck");
    assertNotNull(component);
    AbstractLuceneSpellChecker spellChecker;
    Comparator<SuggestWord> comp;
    spellChecker = (AbstractLuceneSpellChecker) component.getSpellChecker("freq");
    assertNotNull(spellChecker);
    comp = spellChecker.getSpellChecker().getComparator();
    assertNotNull(comp);
    assertTrue(comp instanceof SuggestWordFrequencyComparator);

    spellChecker = (AbstractLuceneSpellChecker) component.getSpellChecker("fqcn");
    assertNotNull(spellChecker);
    comp = spellChecker.getSpellChecker().getComparator();
    assertNotNull(comp);
    assertTrue(comp instanceof SampleComparator);
  }

  @Test
  public void testSpelling() throws Exception {
    IndexBasedSpellChecker checker = new IndexBasedSpellChecker();

    NamedList<Object> spellchecker = new NamedList<>();
    spellchecker.add("classname", IndexBasedSpellChecker.class.getName());

    Path indexDir = createTempDir();

    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, indexDir.toString());
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "title");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);
    SolrCore core = h.getCore();

    String dictName = checker.init(spellchecker, core);
    assertEquals(
        dictName + " is not equal to " + SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        dictName);
    h.getCore()
        .withSearcher(
            searcher -> {
              checker.build(core, searcher);

              IndexReader reader = searcher.getIndexReader();
              Supplier<TokenStream> tokenStreamSupplier = () -> queryConverter.convert("documemt");
              SpellingOptions spellOpts = new SpellingOptions(tokenStreamSupplier, reader);
              SpellingResult result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              // should be lowercased, b/c we are using a lowercasing analyzer
              Map<String, Integer> suggestions =
                  result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNotNull("documemt is null and it shouldn't be", suggestions);
              assertEquals(
                  "documemt Size: " + suggestions.size() + " is not: " + 1, 1, suggestions.size());
              Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
              assertEquals(
                  entry.getKey() + " is not equal to " + "document", "document", entry.getKey());
              assertEquals(
                  entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO,
                  SpellingResult.NO_FREQUENCY_INFO,
                  (int) entry.getValue());

              // test something not in the spell checker
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("super");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertEquals("suggestions size should be 0", 0, suggestions.size());

              // test something that is spelled correctly
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("document");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNull("suggestions is null and it shouldn't be", suggestions);

              // Has multiple possibilities, but the exact exists, so that should be returned
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("red");
              spellOpts.count = 2;
              result = checker.getSuggestions(spellOpts);
              assertNotNull(result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNull("suggestions is not null and it should be", suggestions);

              // Try out something which should have multiple suggestions
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("bug");
              result = checker.getSuggestions(spellOpts);
              assertNotNull(result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNotNull(suggestions);
              assertEquals(
                  "suggestions Size: " + suggestions.size() + " is not: " + 2,
                  2,
                  suggestions.size());

              entry = suggestions.entrySet().iterator().next();
              assertNotEquals(
                  entry.getKey() + " is equal to " + "bug and it shouldn't be",
                  "bug",
                  entry.getKey());
              assertEquals(
                  entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO,
                  SpellingResult.NO_FREQUENCY_INFO,
                  (int) entry.getValue());

              entry = suggestions.entrySet().iterator().next();
              assertNotEquals(
                  entry.getKey() + " is equal to " + "bug and it shouldn't be",
                  "bug",
                  entry.getKey());
              assertEquals(
                  entry.getValue() + " does not equal: " + SpellingResult.NO_FREQUENCY_INFO,
                  SpellingResult.NO_FREQUENCY_INFO,
                  (int) entry.getValue());

              // Check empty token due to spellcheck.q = ""
              spellOpts.tokenStreamSupplier = () -> singleEmptyTermTokenStream();
              result = checker.getSuggestions(spellOpts);
              assertNotNull(result);
              suggestions = result.get(new SpellCheckToken("", 0, 0));
              assertNotNull(suggestions);
              assertTrue("suggestions should be empty", suggestions.isEmpty());
              return null;
            });
  }

  /** A stream that emits exactly one token whose term text is empty. */
  private static TokenStream singleEmptyTermTokenStream() {
    return new TokenStream() {
      private boolean done;

      @Override
      public boolean incrementToken() {
        if (done) {
          return false;
        }
        done = true;
        clearAttributes();
        return true;
      }

      @Override
      public void reset() throws IOException {
        super.reset();
        done = false;
      }
    };
  }

  @Test
  public void testExtendedResults() throws Exception {
    IndexBasedSpellChecker checker = new IndexBasedSpellChecker();
    NamedList<Object> spellchecker = new NamedList<>();
    spellchecker.add("classname", IndexBasedSpellChecker.class.getName());

    Path indexDir = createTempDir();
    Files.createDirectories(indexDir);
    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, indexDir.toString());
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "title");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);
    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core);
    assertEquals(
        dictName + " is not equal to " + SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        dictName);
    h.getCore()
        .withSearcher(
            searcher -> {
              checker.build(core, searcher);

              IndexReader reader = searcher.getIndexReader();
              Supplier<TokenStream> tokenStreamSupplier = () -> queryConverter.convert("documemt");
              SpellingOptions spellOpts =
                  new SpellingOptions(
                      tokenStreamSupplier,
                      reader,
                      1,
                      SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
                      true,
                      0.5f,
                      null);
              SpellingResult result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              // should be lowercased, b/c we are using a lowercasing analyzer
              Map<String, Integer> suggestions =
                  result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNotNull("documemt is null and it shouldn't be", suggestions);
              assertEquals(
                  "documemt Size: " + suggestions.size() + " is not: " + 1, 1, suggestions.size());
              Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
              assertEquals(
                  entry.getKey() + " is not equal to " + "document", "document", entry.getKey());
              assertEquals(entry.getValue() + " does not equal: " + 2, 2, (int) entry.getValue());

              // test something not in the spell checker
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("super");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertEquals("suggestions size should be 0", 0, suggestions.size());

              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("document");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNull("suggestions is not null and it should be", suggestions);
              return null;
            });
  }

  private static class TestSpellChecker extends IndexBasedSpellChecker {
    @Override
    public SpellChecker getSpellChecker() {
      return spellChecker;
    }
  }

  @Test
  public void testAlternateDistance() throws Exception {
    TestSpellChecker checker = new TestSpellChecker();
    NamedList<Object> spellchecker = new NamedList<>();
    spellchecker.add("classname", IndexBasedSpellChecker.class.getName());

    Path indexDir = createTempDir();
    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, indexDir.toString());
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "title");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);
    spellchecker.add(
        AbstractLuceneSpellChecker.STRING_DISTANCE, JaroWinklerDistance.class.getName());
    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core);
    assertEquals(
        dictName + " is not equal to " + SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        dictName);
    h.getCore()
        .withSearcher(
            searcher -> {
              checker.build(core, searcher);
              SpellChecker sc = checker.getSpellChecker();
              assertNotNull("sc is null and it shouldn't be", sc);
              StringDistance sd = sc.getStringDistance();
              assertNotNull("sd is null and it shouldn't be", sd);
              assertTrue(
                  "sd is not an instance of " + JaroWinklerDistance.class.getName(),
                  sd instanceof JaroWinklerDistance);
              return null;
            });
  }

  @Test
  public void testAlternateLocation() throws Exception {
    String[] ALT_DOCS =
        new String[] {
          "jumpin jack flash",
          "Sargent Peppers Lonely Hearts Club Band",
          "Born to Run",
          "Thunder Road",
          "Londons Burning",
          "A Horse with No Name",
          "Sweet Caroline"
        };

    IndexBasedSpellChecker checker = new IndexBasedSpellChecker();
    NamedList<Object> spellchecker = new NamedList<>();
    spellchecker.add("classname", IndexBasedSpellChecker.class.getName());

    Path tmpDir = createTempDir();
    Path indexDir = tmpDir.resolve("spellingIdx");
    // create a standalone index
    Path altIndexDir = tmpDir.resolve("alternateIdx" + new Date().getTime());
    Directory dir = newFSDirectory(altIndexDir);
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()));
    for (String alt_doc : ALT_DOCS) {
      Document doc = new Document();
      doc.add(new TextField("title", alt_doc, Field.Store.YES));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    dir.close();
    Files.createDirectories(indexDir);
    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, indexDir.toString());
    spellchecker.add(AbstractLuceneSpellChecker.LOCATION, altIndexDir.toString());
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "title");
    spellchecker.add(AbstractLuceneSpellChecker.SPELLCHECKER_ARG_NAME, spellchecker);
    SolrCore core = h.getCore();
    String dictName = checker.init(spellchecker, core);
    assertEquals(
        dictName + " is not equal to " + SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        SolrSpellChecker.DEFAULT_DICTIONARY_NAME,
        dictName);
    h.getCore()
        .withSearcher(
            searcher -> {
              checker.build(core, searcher);

              IndexReader reader = searcher.getIndexReader();
              Supplier<TokenStream> tokenStreamSupplier = () -> queryConverter.convert("flesh");
              SpellingOptions spellOpts =
                  new SpellingOptions(
                      tokenStreamSupplier,
                      reader,
                      1,
                      SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
                      true,
                      0.5f,
                      null);
              SpellingResult result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              // should be lowercased, b/c we are using a lowercasing analyzer
              Map<String, Integer> suggestions =
                  result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNotNull("flesh is null and it shouldn't be", suggestions);
              assertEquals(
                  "flesh Size: " + suggestions.size() + " is not: " + 1, 1, suggestions.size());
              Map.Entry<String, Integer> entry = suggestions.entrySet().iterator().next();
              assertEquals(entry.getKey() + " is not equal to " + "flash", "flash", entry.getKey());
              assertEquals(entry.getValue() + " does not equal: " + 1, 1, (int) entry.getValue());

              // test something not in the spell checker
              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("super");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertEquals("suggestions size should be 0", 0, suggestions.size());

              spellOpts.tokenStreamSupplier = () -> queryConverter.convert("Caroline");
              result = checker.getSuggestions(spellOpts);
              assertNotNull("result is null and it shouldn't be", result);
              suggestions = result.get(firstToken(spellOpts.tokenStreamSupplier));
              assertNull("suggestions is not null and it should be", suggestions);
              return null;
            });
  }
}
