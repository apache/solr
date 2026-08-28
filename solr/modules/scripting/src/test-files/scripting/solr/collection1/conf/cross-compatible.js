// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

function get_class(name) {
   var clazz;
   try {
     // Java8 Nashorn
     clazz = eval("Java.type(name).class");
   } catch(e) {
     // Java7 Rhino
     clazz = eval("Packages."+name);
   }

   return clazz;
}

function processAdd(cmd) {
  var doc = cmd.getSolrInputDocument();

  var analyzer =
       req.getCore().getLatestSchema()
       .getFieldTypeByName("text")
       .getIndexAnalyzer();

  var token_stream =
       analyzer.tokenStream("subject", doc.getFieldValue("subject"));

  var cta_class = get_class("org.apache.lucene.analysis.tokenattributes.CharTermAttribute");
  var term_att = token_stream.getAttribute(cta_class);
  token_stream.reset();
  while (token_stream.incrementToken()) {
    doc.addField("term_s", term_att.toString());
  }
  token_stream.end();
  token_stream.close();

  return true;
}

// // //

function processDelete() {
    // NOOP
}
function processCommit() {
    // NOOP
}
function processRollback() {
    // NOOP
}
function processMergeIndexes() {
    // NOOP
}
function finish() {
    // NOOP
}
