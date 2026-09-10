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

var Assert = Packages.org.junit.Assert;

function processAdd(cmd) {
    functionMessages.add("processAdd0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
    Assert.assertNotNull(params);
    Assert.assertTrue(1 == params.get('intValue').intValue());  // had issues with assertTrue(1, params.get('intValue').intValue()) casting to wrong variant
    Assert.assertTrue(params.get('boolValue').booleanValue());

    // Integer.valueOf is needed here to get a tru java object, because
    // all javascript numbers are floating point (ie: java.lang.Double)
    cmd.getSolrInputDocument().addField("script_added_i",
                                        java.lang.Integer.valueOf(42));
    cmd.getSolrInputDocument().addField("script_added_d", 42.3);

}

function processDelete(cmd) {
    functionMessages.add("processDelete0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processMergeIndexes(cmd) {
    functionMessages.add("processMergeIndexes0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processCommit(cmd) {
    functionMessages.add("processCommit0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function processRollback(cmd) {
    functionMessages.add("processRollback0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
    Assert.assertNotNull(cmd);
}

function finish() {
    functionMessages.add("finish0");
    Assert.assertNotNull(req);
    Assert.assertNotNull(rsp);
    Assert.assertNotNull(logger);
}

