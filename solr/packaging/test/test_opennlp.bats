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
  solr stop --all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure
}

# This BATS style test is really just to help explore the space of Modern NLP in
# Apache Solr, versus a "true" integration test that I want to have run regularly.
# On the other hand, since integrating NLP requires a lot of steps, maybe having this
# long test as an "integration" test is something we decide is okay?
# I also have dreams of incorporating this as code snippets in a Tutorial via the ascii doc tags
# like we use for the SolrJ code snippets.  That way we know the snippets continue to work!
@test "Check lifecycle of sentiment classification" {

  echo "Downloading onnx model and vocab..."
  
  mkdir -p ${SOLR_TIP}/models/sentiment/
  
  # Models originally sourced from Huggingface and store on ASF hardware
  wget -O ${SOLR_TIP}/models/sentiment/model.onnx https://nightlies.apache.org/solr/opennlp-dl-integration/model_quantized.onnx
  wget -O ${SOLR_TIP}/models/sentiment/vocab.txt https://nightlies.apache.org/solr/opennlp-dl-integration/vocab.txt
  # wget -O ${SOLR_TIP}/models/sentiment/model.onnx https://huggingface.co/onnx-community/bert-base-multilingual-uncased-sentiment-ONNX/resolve/main/onnx/model_quantized.onnx
  # wget -O ${SOLR_TIP}/models/sentiment/vocab.txt https://huggingface.co/onnx-community/bert-base-multilingual-uncased-sentiment-ONNX/raw/main/vocab.txt

  run ls -alh ${SOLR_TIP}/modules/analysis-extras/lib
  refute_output --partial "onnxruntime_gpu"

    # Can't figure out magic policy stuff to allow loading ONNX, so disable security manager.
  export SOLR_SECURITY_MANAGER_ENABLED=false

  solr start -m 4g -Dsolr.modules=analysis-extras -Dsolr.packages.enabled=true
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000

  run solr create -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"

  curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field":{
      "name":"name",
      "type":"string",
      "stored":true }
  }' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/schema"

  curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field":{
      "name":"name_sentiment",
      "type":"string",
      "stored":true }
  }' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/schema"

  run curl --data-binary @${SOLR_TIP}/models/sentiment/vocab.txt -X PUT "http://localhost:${SOLR_PORT}/api/cluster/filestore/files/models/sentiment/vocab.txt"
  assert_output --partial '"status":0'

  run curl --data-binary @${SOLR_TIP}/models/sentiment/model.onnx -X PUT "http://localhost:${SOLR_PORT}/api/cluster/filestore/files/models/sentiment/model.onnx"
  assert_output --partial '"status":0'

  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-updateprocessor": {
      "name": "sentimentClassifier",
      "class": "solr.processor.DocumentCategorizerUpdateProcessorFactory",
      "modelFile": "models/sentiment/model.onnx",
      "vocabFile": "models/sentiment/vocab.txt",
      "source": "name",
      "dest": "name_sentiment"
    }
  }' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/config"
  assert_output --partial '"status":0'

  run curl -X POST -H 'Content-type:application/json' -d '[
    {
      "id":"good",
      "name" : "that was an awesome movie!"
    },
    {
      "id":"bad",
      "name" : "that movie was bad and terrible"
    }
  ]' "http://localhost:${SOLR_PORT}/solr/COLL_NAME/update/json?processor=sentimentClassifier&commit=true"

  assert_output --partial '"status":0'

  run curl -X GET "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=id:good"
  assert_output --partial '"name_sentiment":"very good"'

  run curl -X GET "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=id:bad"
  assert_output --partial '"name_sentiment":"very bad"'
}
