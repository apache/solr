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
package org.apache.solr.bert.search;

import com.robrua.nlp.bert.Bert;
import java.io.Closeable;
import java.io.File;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.neural.KnnQParser;
import org.apache.solr.util.SolrPluginUtils;

public class BertKnnQParserPlugin extends QParserPlugin implements Closeable {

  public static final String NAME = "bert";

  private File modelFile;

  Bert bert;

  public void setModelFile(String modelFile) {
    this.modelFile = new File(modelFile);
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);

    SolrPluginUtils.invokeSetters(this, args);

    if (this.modelFile != null) {
      try {
        this.bert = Bert.load(modelFile);
      } catch (Throwable t) {
        // TODO: log something helpful and re-throw
      }
    } else {
      // TODO: throw helpful exception
    }
  }

  @Override
  public void close() {
    if (bert != null) {
      bert.close();
      bert = null;
    }
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new BertKnnQParser(qstr, localParams, params, req);
  }

  public class BertKnnQParser extends KnnQParser {

    public BertKnnQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() {
      return implParse("Text");
    }

    @Override
    protected float[] toVector(String value, int dimension) {
      if (bert == null) {
        // TODO: throw instead?
        return new float[dimension];
      }
      float[] elements = bert.embedSequence(value);
      if (elements.length != dimension) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "incorrect vector dimension:"
                + " actual="
                + elements.length
                + " expected="
                + dimension);
      }
      return elements;
    }
  }
}
