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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.SolrCoreAware;

public class CuvsCodecFactory extends CodecFactory implements SolrCoreAware {

  private final SchemaCodecFactory fallback;
  private SolrCore core;
  NamedList<?> args;
  Lucene101Codec fallbackCodec;
  CuvsCodec codec;

  public CuvsCodecFactory() {
    this.fallback = new SchemaCodecFactory();
  }

  @Override
  public Codec getCodec() {
    if (codec == null) {
      fallbackCodec = (Lucene101Codec) fallback.getCodec();
      codec = new CuvsCodec(core, fallbackCodec, args);
    }
    return codec;
  }

  @Override
  public void inform(SolrCore solrCore) {
    fallback.inform(solrCore);
    this.core = solrCore;
  }

  @Override
  public void init(NamedList<?> args) {
    fallback.init(args);
    this.args = args;
  }
}
