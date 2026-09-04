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
package org.apache.solr.common.params;

/**
 * @since solr 1.3
 */
public interface HighlightParams {
  // primary
  String HIGHLIGHT = "hl";
  String METHOD = HIGHLIGHT + ".method"; // original|fastVector|postings|unified
  String FIELDS = HIGHLIGHT + ".fl";
  String SNIPPETS = HIGHLIGHT + ".snippets";

  //    KEY:
  // OH = (original) Highlighter   (AKA the standard Highlighter)
  // FVH = FastVectorHighlighter
  // UH = UnifiedHighlighter (evolved from PostingsHighlighter)

  // query interpretation
  String Q = HIGHLIGHT + ".q"; // all
  String QPARSER = HIGHLIGHT + ".qparser"; // all
  String FIELD_MATCH = HIGHLIGHT + ".requireFieldMatch"; // OH, FVH, UH
  String QUERY_FIELD_PATTERN = HIGHLIGHT + ".queryFieldPattern"; // UH
  String USE_PHRASE_HIGHLIGHTER = HIGHLIGHT + ".usePhraseHighlighter"; // OH, FVH, UH
  String HIGHLIGHT_MULTI_TERM = HIGHLIGHT + ".highlightMultiTerm"; // all

  // if no snippets...
  String DEFAULT_SUMMARY = HIGHLIGHT + ".defaultSummary"; // UH
  String ALTERNATE_FIELD = HIGHLIGHT + ".alternateField"; // OH, FVH
  String ALTERNATE_FIELD_LENGTH = HIGHLIGHT + ".maxAlternateFieldLength"; // OH, FVH
  String HIGHLIGHT_ALTERNATE = HIGHLIGHT + ".highlightAlternate"; // OH, FVH

  // sizing
  String FRAGSIZE = HIGHLIGHT + ".fragsize"; // OH, FVH, UH
  String FRAGSIZEISMINIMUM = HIGHLIGHT + ".fragsizeIsMinimum"; // UH
  String FRAGALIGNRATIO = HIGHLIGHT + ".fragAlignRatio"; // UH
  String FRAGMENTER = HIGHLIGHT + ".fragmenter"; // OH
  String INCREMENT = HIGHLIGHT + ".increment"; // OH
  String REGEX = "regex"; // OH
  String SLOP = HIGHLIGHT + "." + REGEX + ".slop"; // OH
  String PATTERN = HIGHLIGHT + "." + REGEX + ".pattern"; // OH
  String MAX_RE_CHARS = HIGHLIGHT + "." + REGEX + ".maxAnalyzedChars"; // OH
  String BOUNDARY_SCANNER = HIGHLIGHT + ".boundaryScanner"; // FVH
  String BS_MAX_SCAN = HIGHLIGHT + ".bs.maxScan"; // FVH
  String BS_CHARS = HIGHLIGHT + ".bs.chars"; // FVH
  String BS_TYPE = HIGHLIGHT + ".bs.type"; // FVH, UH
  String BS_LANGUAGE = HIGHLIGHT + ".bs.language"; // FVH, UH
  String BS_COUNTRY = HIGHLIGHT + ".bs.country"; // FVH, UH
  String BS_VARIANT = HIGHLIGHT + ".bs.variant"; // FVH, UH
  String BS_SEP = HIGHLIGHT + ".bs.separator"; // UH

  // formatting
  String FORMATTER = HIGHLIGHT + ".formatter"; // OH
  String ENCODER = HIGHLIGHT + ".encoder"; // all
  String MERGE_CONTIGUOUS_FRAGMENTS = HIGHLIGHT + ".mergeContiguous"; // OH
  String SIMPLE = "simple"; // OH
  String SIMPLE_PRE = HIGHLIGHT + "." + SIMPLE + ".pre"; // OH
  String SIMPLE_POST = HIGHLIGHT + "." + SIMPLE + ".post"; // OH
  String FRAGMENTS_BUILDER = HIGHLIGHT + ".fragmentsBuilder"; // FVH
  String TAG_PRE = HIGHLIGHT + ".tag.pre"; // FVH, UH
  String TAG_POST = HIGHLIGHT + ".tag.post"; // FVH, UH
  String TAG_ELLIPSIS = HIGHLIGHT + ".tag.ellipsis"; // FVH, UH
  String MULTI_VALUED_SEPARATOR = HIGHLIGHT + ".multiValuedSeparatorChar"; // FVH

  // ordering
  String PRESERVE_MULTI = HIGHLIGHT + ".preserveMulti"; // OH
  String FRAG_LIST_BUILDER = HIGHLIGHT + ".fragListBuilder"; // FVH
  String SCORE = "score"; // UH
  String SCORE_K1 = HIGHLIGHT + "." + SCORE + ".k1"; // UH
  String SCORE_B = HIGHLIGHT + "." + SCORE + ".b"; // UH
  String SCORE_PIVOT = HIGHLIGHT + "." + SCORE + ".pivot"; // UH

  // misc
  String MAX_CHARS = HIGHLIGHT + ".maxAnalyzedChars"; // all
  String PAYLOADS = HIGHLIGHT + ".payloads"; // OH
  String MAX_MULTIVALUED_TO_EXAMINE = HIGHLIGHT + ".maxMultiValuedToExamine"; // OH
  String MAX_MULTIVALUED_TO_MATCH = HIGHLIGHT + ".maxMultiValuedToMatch"; // OH
  String PHRASE_LIMIT = HIGHLIGHT + ".phraseLimit"; // FVH
  String OFFSET_SOURCE = HIGHLIGHT + ".offsetSource"; // UH
  String CACHE_FIELD_VAL_CHARS_THRESHOLD = HIGHLIGHT + ".cacheFieldValCharsThreshold"; // UH
  String WEIGHT_MATCHES = HIGHLIGHT + ".weightMatches"; // UH
}
