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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import ai.onnxruntime.OrtException;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import opennlp.dl.InferenceOptions;
import opennlp.dl.doccat.DocumentCategorizerDL;
import opennlp.dl.doccat.scoring.AverageClassificationScoringStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.SolrCore;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentCategorizerUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOURCE_PARAM = "source";
  public static final String DEST_PARAM = "dest";
  public static final String PATTERN_PARAM = "pattern";
  public static final String REPLACEMENT_PARAM = "replacement";
  public static final String MODEL_PARAM = "modelFile";
  public static final String VOCAB_PARAM = "vocabFile";

  private Path solrHome;

  private SelectorParams srcInclusions = new SelectorParams();
  private Collection<SelectorParams> srcExclusions = new ArrayList<>();

  private FieldNameSelector srcSelector = null;

  private String model = null;
  private String vocab = null;
  private String analyzerFieldType = null;

  /**
   * If pattern is null, this this is a literal field name. If pattern is non-null then this is a
   * replacement string that may contain meta-characters (ie: capture group identifiers)
   *
   * @see #pattern
   */
  private String dest = null;
  /**
   * @see #dest
   */
  private Pattern pattern = null;

  protected final FieldNameSelector getSourceSelector() {
    if (null != srcSelector) return srcSelector;

    throw new SolrException(
        SERVER_ERROR, "selector was never initialized, inform(SolrCore) never called???");
  }

  @Override
  public void init(NamedList<?> args) {

    System.out.println("In OpenNLP doccat init()");

    // high level (loose) check for which type of config we have.
    //
    // individual init methods do more strict syntax checking
    if (0 <= args.indexOf(SOURCE_PARAM, 0) && 0 <= args.indexOf(DEST_PARAM, 0)) {
      initSourceSelectorSyntax(args);
    } else if (0 <= args.indexOf(PATTERN_PARAM, 0) && 0 <= args.indexOf(REPLACEMENT_PARAM, 0)) {
      initSimpleRegexReplacement(args);
    } else {
      throw new SolrException(
          SERVER_ERROR,
          "A combination of either '"
              + SOURCE_PARAM
              + "' + '"
              + DEST_PARAM
              + "', or '"
              + REPLACEMENT_PARAM
              + "' + '"
              + PATTERN_PARAM
              + "' init params are mandatory");
    }

    Object modelParam = args.remove(MODEL_PARAM);
    if (null == modelParam) {
      throw new SolrException(SERVER_ERROR, "Missing required init param '" + MODEL_PARAM + "'");
    }
    if (!(modelParam instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + MODEL_PARAM + "' must be a <str>");
    }
    model = modelParam.toString();
    System.out.println("In OpenNLP doccat - model: " + model);

    Object vocabParam = args.remove(VOCAB_PARAM);
    if (null == vocabParam) {
      throw new SolrException(SERVER_ERROR, "Missing required init param '" + VOCAB_PARAM + "'");
    }
    if (!(vocabParam instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + VOCAB_PARAM + "' must be a <str>");
    }
    vocab = vocabParam.toString();

    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR, "Unexpected init param(s): '" + args.getName(0) + "'");
    }

    super.init(args);
  }

  /**
   * init helper method that should only be called when we know for certain that both the "source"
   * and "dest" init params do <em>not</em> exist.
   */
  private void initSimpleRegexReplacement(NamedList<?> args) {
    // The syntactic sugar for the case where there is only one regex pattern for source and the
    // same pattern
    // is used for the destination pattern...
    //
    //  pattern != null && replacement != null
    //
    // ...as top level elements, with no other config options specified

    // if we got here we know we had pattern and replacement, now check for the other two  so that
    // we can give a better
    // message than "unexpected"
    if (0 <= args.indexOf(SOURCE_PARAM, 0) || 0 <= args.indexOf(DEST_PARAM, 0)) {
      throw new SolrException(
          SERVER_ERROR,
          "Short hand syntax must not be mixed with full syntax. Found "
              + PATTERN_PARAM
              + " and "
              + REPLACEMENT_PARAM
              + " but also found "
              + SOURCE_PARAM
              + " or "
              + DEST_PARAM);
    }

    assert args.indexOf(SOURCE_PARAM, 0) < 0;

    Object patt = args.remove(PATTERN_PARAM);
    Object replacement = args.remove(REPLACEMENT_PARAM);

    if (null == patt || null == replacement) {
      throw new SolrException(
          SERVER_ERROR,
          "Init params '"
              + PATTERN_PARAM
              + "' and '"
              + REPLACEMENT_PARAM
              + "' are both mandatory if '"
              + SOURCE_PARAM
              + "' and '"
              + DEST_PARAM
              + "' are not both specified");
    }

    if (0 != args.size()) {
      throw new SolrException(
          SERVER_ERROR,
          "Init params '"
              + REPLACEMENT_PARAM
              + "' and '"
              + PATTERN_PARAM
              + "' must be children of '"
              + DEST_PARAM
              + "' to be combined with other options.");
    }

    if (!(replacement instanceof String)) {
      throw new SolrException(
          SERVER_ERROR, "Init param '" + REPLACEMENT_PARAM + "' must be a string (i.e. <str>)");
    }
    if (!(patt instanceof String)) {
      throw new SolrException(
          SERVER_ERROR, "Init param '" + PATTERN_PARAM + "' must be a string (i.e. <str>)");
    }

    dest = replacement.toString();
    try {
      this.pattern = Pattern.compile(patt.toString());
    } catch (PatternSyntaxException pe) {
      throw new SolrException(
          SERVER_ERROR,
          "Init param " + PATTERN_PARAM + " is not a valid regex pattern: " + patt,
          pe);
    }
    srcInclusions = new SelectorParams();
    srcInclusions.fieldRegex = Collections.singletonList(this.pattern);
  }

  /**
   * init helper method that should only be called when we know for certain that both the "source"
   * and "dest" init params <em>do</em> exist.
   */
  private void initSourceSelectorSyntax(NamedList<?> args) {
    // Full and complete syntax where source and dest are mandatory.
    //
    // source may be a single string or a selector.
    // dest may be a single string or list containing pattern and replacement
    //
    //   source != null && dest != null

    // if we got here we know we had source and dest, now check for the other two so that we can
    // give a better
    // message than "unexpected"
    if (0 <= args.indexOf(PATTERN_PARAM, 0) || 0 <= args.indexOf(REPLACEMENT_PARAM, 0)) {
      throw new SolrException(
          SERVER_ERROR,
          "Short hand syntax must not be mixed with full syntax. Found "
              + SOURCE_PARAM
              + " and "
              + DEST_PARAM
              + " but also found "
              + PATTERN_PARAM
              + " or "
              + REPLACEMENT_PARAM);
    }

    Object d = args.remove(DEST_PARAM);
    assert null != d;

    List<?> sources = args.getAll(SOURCE_PARAM);
    assert null != sources;

    if (1 == sources.size()) {
      if (sources.get(0) instanceof NamedList) {
        // nested set of selector options
        NamedList<?> selectorConfig = (NamedList<?>) args.remove(SOURCE_PARAM);

        srcInclusions = parseSelectorParams(selectorConfig);

        List<?> excList = selectorConfig.getAll("exclude");

        for (Object excObj : excList) {
          if (null == excObj) {
            throw new SolrException(
                SERVER_ERROR, "Init param '" + SOURCE_PARAM + "' child 'exclude' can not be null");
          }
          if (!(excObj instanceof NamedList)) {
            throw new SolrException(
                SERVER_ERROR, "Init param '" + SOURCE_PARAM + "' child 'exclude' must be <lst/>");
          }
          NamedList<?> exc = (NamedList<?>) excObj;
          srcExclusions.add(parseSelectorParams(exc));
          if (0 < exc.size()) {
            throw new SolrException(
                SERVER_ERROR,
                "Init param '"
                    + SOURCE_PARAM
                    + "' has unexpected 'exclude' sub-param(s): '"
                    + selectorConfig.getName(0)
                    + "'");
          }
          // call once per instance
          selectorConfig.remove("exclude");
        }

        if (0 < selectorConfig.size()) {
          throw new SolrException(
              SERVER_ERROR,
              "Init param '"
                  + SOURCE_PARAM
                  + "' contains unexpected child param(s): '"
                  + selectorConfig.getName(0)
                  + "'");
        }
        // consume from the named list so it doesn't interfere with subsequent processing
        sources.remove(0);
      }
    }
    if (1 <= sources.size()) {
      // source better be one or more strings
      srcInclusions.fieldName = new HashSet<>(args.removeConfigArgs("source"));
    }
    if (srcInclusions == null) {
      throw new SolrException(
          SERVER_ERROR,
          "Init params do not specify any field from which to extract entities, please supply either "
              + SOURCE_PARAM
              + " and "
              + DEST_PARAM
              + " or "
              + PATTERN_PARAM
              + " and "
              + REPLACEMENT_PARAM
              + ". See javadocs"
              + "for OpenNLPExtractNamedEntitiesUpdateProcessor for further details.");
    }

    if (d instanceof NamedList) {
      NamedList<?> destList = (NamedList<?>) d;

      Object patt = destList.remove(PATTERN_PARAM);
      Object replacement = destList.remove(REPLACEMENT_PARAM);

      if (null == patt || null == replacement) {
        throw new SolrException(
            SERVER_ERROR,
            "Init param '"
                + DEST_PARAM
                + "' children '"
                + PATTERN_PARAM
                + "' and '"
                + REPLACEMENT_PARAM
                + "' are both mandatory and can not be null");
      }
      if (!(patt instanceof String && replacement instanceof String)) {
        throw new SolrException(
            SERVER_ERROR,
            "Init param '"
                + DEST_PARAM
                + "' children '"
                + PATTERN_PARAM
                + "' and '"
                + REPLACEMENT_PARAM
                + "' must both be strings (i.e. <str>)");
      }
      if (0 != destList.size()) {
        throw new SolrException(
            SERVER_ERROR,
            "Init param '"
                + DEST_PARAM
                + "' has unexpected children: '"
                + destList.getName(0)
                + "'");
      }

      try {
        this.pattern = Pattern.compile(patt.toString());
      } catch (PatternSyntaxException pe) {
        throw new SolrException(
            SERVER_ERROR,
            "Init param '"
                + DEST_PARAM
                + "' child '"
                + PATTERN_PARAM
                + " is not a valid regex pattern: "
                + patt,
            pe);
      }
      dest = replacement.toString();

    } else if (d instanceof String) {
      dest = d.toString();
    } else {
      throw new SolrException(
          SERVER_ERROR,
          "Init param '"
              + DEST_PARAM
              + "' must either be a string "
              + "(i.e. <str>) or a list (i.e. <lst>) containing '"
              + PATTERN_PARAM
              + "' and '"
              + REPLACEMENT_PARAM);
    }
  }

  @Override
  public void inform(final SolrCore core) {
    this.solrHome = Paths.get(core.getCoreContainer().getSolrHome());
    srcSelector =
        FieldMutatingUpdateProcessor.createFieldNameSelector(
            core.getResourceLoader(),
            core,
            srcInclusions,
            FieldMutatingUpdateProcessor.SELECT_NO_FIELDS);

    for (SelectorParams exc : srcExclusions) {
      srcSelector =
          FieldMutatingUpdateProcessor.wrap(
              srcSelector,
              FieldMutatingUpdateProcessor.createFieldNameSelector(
                  core.getResourceLoader(),
                  core,
                  exc,
                  FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
    }
  }

  @Override
  public final UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    final FieldNameSelector srcSelector = getSourceSelector();
    return new UpdateRequestProcessor(next) {

      DocumentCategorizerDL documentCategorizerDL = null;

      {
        // Initialize the categorizer.

        var path = solrHome.resolve(PackageStoreAPI.PACKAGESTORE_DIRECTORY);
        File modelFile = new File(model);
        File vocabFile = new File(vocab);

        if (!Files.exists(modelFile.toPath())) {
          System.out.println("modelFile doesnt exist:" + modelFile.toPath());
          modelFile = new File(path + "/" + model);
          System.out.println("New file:" + modelFile);
        }
        if (!Files.exists(vocabFile.toPath())) {
          System.out.println("vocabFile doesnt exist:" + vocabFile.toPath());
          vocabFile = new File(path + "/" + vocab);
        }

        System.out.println("model is " + model);
        System.out.println("does modelFile exist?" + modelFile.exists());
        System.out.println("model full path is " + modelFile.getAbsolutePath());

        System.out.println("In OpenNLP doccat initializing the documentCategorizerDL");
        try {
          documentCategorizerDL =
              new DocumentCategorizerDL(
                  modelFile,
                  vocabFile,
                  getCategories(),
                  new AverageClassificationScoringStrategy(),
                  new InferenceOptions());
        } catch (IOException e) {
          e.printStackTrace();
        } catch (OrtException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {

        final SolrInputDocument doc = cmd.getSolrInputDocument();

        // Destination may be regex replace string, or "{EntityType}" replaced by
        // each entity's type, both of which can cause multiple output fields.
        Map<String, SolrInputField> destMap = new HashMap<>();

        // preserve initial values
        for (final String fname : doc.getFieldNames()) {
          if (!srcSelector.shouldMutate(fname)) continue;

          Collection<Object> srcFieldValues = doc.getFieldValues(fname);
          if (srcFieldValues == null || srcFieldValues.isEmpty()) continue;

          String resolvedDest = dest;

          if (pattern != null) {
            Matcher matcher = pattern.matcher(fname);
            if (matcher.find()) {
              resolvedDest = matcher.replaceAll(dest);
            } else {
              log.debug(
                  "srcSelector.shouldMutate('{}') returned true, "
                      + "but replacement pattern did not match, field skipped.",
                  fname);
              continue;
            }
          }

          for (Object val : srcFieldValues) {
            for (Pair<String, String> entity : classify(val)) {
              SolrInputField destField = null;
              // String classification = entity.first();
              String classificationValue = entity.second();
              final String resolved = resolvedDest;
              if (doc.containsKey(resolved)) {
                destField = doc.getField(resolved);
              } else {
                SolrInputField targetField = destMap.get(resolved);
                if (targetField == null) {
                  destField = new SolrInputField(resolved);
                } else {
                  destField = targetField;
                }
              }
              destField.addValue(classificationValue);

              // put it in map to avoid concurrent modification...
              destMap.put(resolved, destField);
            }
          }
        }

        for (Map.Entry<String, SolrInputField> entry : destMap.entrySet()) {
          doc.put(entry.getKey(), entry.getValue());
        }
        super.processAdd(cmd);
      }

      private List<Pair<String, String>> classify(Object srcFieldValue) {

        String fullText = srcFieldValue.toString();

        // Send the fullText to the model for classification.
        System.out.println("In OpenNLP doccat calling categorizer()");
        final double[] result = documentCategorizerDL.categorize(new String[] {fullText});

        // Add the categories to the list and return it.
        // Just take the top category for now.
        // TODO: Allow for a threshold value for returning categories.

        List<Pair<String, String>> classifications = new ArrayList<>();

        String bestCategory = documentCategorizerDL.getBestCategory(result);
        System.out.println("In OpenNLP doccat - best category = " + bestCategory);

        Pair<String, String> pair = new Pair<>("classification", bestCategory);
        classifications.add(pair);

        return classifications;
      }
    };
  }

  /** macro */
  private static SelectorParams parseSelectorParams(NamedList<?> args) {
    return FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
  }

  private Map<Integer, String> getCategories() {

    // TODO: Read these from the Solr config for the processor.

    final Map<Integer, String> categories = new HashMap<>();

    categories.put(0, "very bad");
    categories.put(1, "bad");
    categories.put(2, "neutral");
    categories.put(3, "good");
    categories.put(4, "very good");

    return categories;
  }
}
