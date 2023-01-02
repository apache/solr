<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Apache Solr - Analysis Extras
=============================

The analysis-extras plugin provides additional analyzers that rely
upon large dependencies/dictionaries.

It includes integration with ICU for multilingual support,
analyzers for Chinese and Polish, and integration with
OpenNLP for multilingual tokenization, part-of-speech tagging
lemmatization, phrase chunking, and named-entity recognition.

Each of the jars below relies upon including `/modules/analysis-extras/lib/solr-analysis-extras-X.Y.Z.jar`
in the `solrconfig.xml`

* ICU relies upon `lib/lucene-analyzers-icu-X.Y.jar`
and `lib/icu4j-X.Y.jar`

* Smartcn relies upon `lib/lucene-analyzers-smartcn-X.Y.jar`

* Stempel relies on `lib/lucene-analyzers-stempel-X.Y.jar`

* Morfologik relies on `lib/lucene-analyzers-morfologik-X.Y.jar`
and `lib/morfologik-*.jar`

* OpenNLP relies on `lib/lucene-analyzers-opennlp-X.Y.jar`
and `lib/opennlp-*.jar`
