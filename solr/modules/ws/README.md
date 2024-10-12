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

Apache Solr - Web Socket Interface
===========================

This module adds a websocket interface to Apache Solr. 

Currently supported operations: 
 - NONE!

# Getting Started

soon...

# Status

This module is extremely experimental and subject to complete overhaul at any time.
No backwards compatability or forward compatability is guaranteed yet. Anything could chnage.

Currently this is a hacked in version of the netty example web socket server found here:
https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/http/websocketx/server

This is just proof of concept stuff. Points of interest:
 - I have engineerd a mechanism for moules to run inialization code. 
   - Implement ModuleLifecycle
   - create a one line file at `META-INF/modulces/<name>/init-class.txt` with the FQN of your implementation
   - The start method will be run async via `CoreContainer.runAsync()` and receive a reference to the core container
   - The shutdown method will run synchronously as the first step in `CoreContainer.shutdown()`
 - The incomming websocket connection parses the input as JSON, and reports a Json response
 - The example I co-opted from netty contains a rudimentary input/output form to use for testing
   - this is found at http://127.0.0.1:9983/ (solr port + 1000) if you start solr with -Dsolr.modules=ws
   - For unknown reasons the websocket can take a while to start, sometmes up to 2 minutes. That obviously needs fixed.
 - This does not YET demonstrate any query or indexing functionality.
 - No efort or testing of any ssl has been done yet.

The one line init-class.txt specification was chosen with the thought that it was a way to avoid something costly
like annotation scanning, and maybe simpler than a standard service loader type of thing. That's just an initial
idea and subject to revision, however.