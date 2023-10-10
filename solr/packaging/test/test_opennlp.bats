#!/usr/bin/env bats

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load bats_helper

setup_file() {
  common_clean_setup
  
}

teardown_file() {
  common_setup
  solr stop -all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure
}

@test "Check lifecycle of sentiment classification" {
  
  # GPU versions is linux and windows only, not OSX.  So swap jars.
  rm -f ${SOLR_TIP}/modules/analysis-extras/lib/onnxruntime_gpu-1.14.0.jar
  # restore 
  #curl --insecure -o ${SOLR_TIP}/modules/analysis-extras/lib/onnxruntime-1.14.0.jar https://repo1.maven.org/maven2/com/microsoft/onnxruntime/onnxruntime/1.14.0/onnxruntime-1.14.0.jar
  cp /Users/epugh/Documents/projects/solr-epugh/onnxruntime-1.14.0.jar ${SOLR_TIP}/modules/analysis-extras/lib/
  
  run ls -alh ${SOLR_TIP}/modules/analysis-extras/lib  
  refute_output --partial "onnxruntime_gpu"
  assert_output --partial "onnxruntime-1.14.0.jar"
  
    # Can't figure out magic policy stuff to allow loading ONNX, so disable security manager.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  
  solr start -c -Dsolr.modules=analysis-extras
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000
  
  run solr create -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"
  
  curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field":{
      "name":"name",
      "type":"string",
      "stored":true }
  }' http://localhost:${SOLR_PORT}/solr/COLL_NAME/schema
  
  curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field":{
      "name":"name_sentiment",
      "type":"string",
      "stored":true }
  }' http://localhost:${SOLR_PORT}/solr/COLL_NAME/schema  
    
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-updateprocessor": {
      "name": "sentimentClassifier",
      "class": "solr.processor.OpenNLPDoccatUpdateProcessorFactory",
      "modelFile": "/Users/epugh/Documents/projects/solr-epugh/exported/model.onnx",
      "vocabFile": "/Users/epugh/Documents/projects/solr-epugh/exported/vocab.txt",
      "source": "name",
      "dest": "name_sentiment"
    }
  }' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/config"
  assert_output --partial '"status":0'
  
  run curl -X POST -H 'Content-type:application/json'd  -d '[
    {
      "id":"good",
      "name" : "Jeff, i am so glad you came to this conference."
    },
    {
      "id":"bad",
      "name" : "The name of this conference is really really terrible to say."
    }
  ]' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/update/json?update.chain=onnx-opennlp&commit=true"
  
  assert_output --partial '"status":0'
  
  run curl -X GET "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*"
  
  assert_output --partial "dude"
}
