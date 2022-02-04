/*
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
 */
//helper for formatting JSON and others

solrAdminApp.controller('ParamSetsController',
  function($scope, $routeParams, ParamSet, Constants) {
    $scope.resetMenu("paramsets", Constants.IS_COLLECTION_PAGE);

    $scope.refresh = function () {
      $scope.commitWithin = 1000;
      $scope.overwrite = true;
      $scope.paramsetContent = "";
      $scope.placeholder = "{\n" +
        "  \"set\": {\n" +
        "    \"myQueries\": {\n" +
        "      \"defType\": \"edismax\",\n" +
        "      \"rows\": \"5\",\n" +
        "      \"df\": \"text_all\"\n" +
        "    }\n" +
        "  }\n" +
        "}"
    };
    $scope.refresh();

    $scope.submit = function () {
      var params = {};

      params.commitWithin = $scope.commitWithin;
      params.overwrite = $scope.overwrite;
      params.core = $routeParams.core;
      params.wt = "json";

      ParamSet.submit(params, $scope.paramsetContent, callback, failure);

      ///////
      function callback(success) {
        $scope.responseStatus = "success";
        delete success.$promise;
        delete success.$resolved;
        $scope.response = JSON.stringify(success, null, '  ');
      }
      function failure (failure) {
        $scope.responseStatus = failure;
      }
    }

    $scope.getParamsets = function () {
      $scope.refresh();

      var params = {};
      params.core = $routeParams.core;
      params.wt = "json";
      params.name = $scope.name;

      ParamSet.get(params, callback, failure);

      ///////

      function callback(success) {
        $scope.responseStatus = "success";
        delete success.$promise;
        delete success.$resolved;
        $scope.response = JSON.stringify(success, null, '  ');
      }

      function failure (failure) {
        $scope.responseStatus = failure;
      }
    }
  });

