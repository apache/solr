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

Welcome to Apache Solr Scripting!
===============================

# Introduction

The Scripting module pulls together various scripting related functions.  

Today, the ScriptUpdateProcessorFactory allows Java scripting engines to support scripts written in languages such as JavaScript, Ruby, Python, and Groovy to be used during Solr document update processing, allowing dramatic flexibility in expressing custom document processing before being indexed.  It exposes hooks for commit, delete, etc, but add is the most common usage.  It is implemented as an UpdateProcessor to be placed in an UpdateChain.

## Getting Started

For information on how to get started please see:
 * [Solr Reference Guide's section on Update Request Processors](https://solr.apache.org/guide/solr/latest/configuration-guide/update-request-processors.html)
 * [Solr Reference Guide's section on ScriptUpdateProcessorFactory](https://solr.apache.org/guide/solr/latest/configuration-guide/script-update-processor.html)
