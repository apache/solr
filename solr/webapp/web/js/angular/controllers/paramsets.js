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

    $scope.paramsetList = [];

    $scope.resetMenu("paramsets", Constants.IS_COLLECTION_PAGE);

    $scope.showHelp = function (id) {
      if ($scope.helpId && ($scope.helpId === id || id === '')) {
        delete $scope.helpId;
      } else {
        $scope.helpId = id;
      }
    };

    $scope.getParamsets = function (isSelected) {
      $scope.refresh();

      var params = {};
      params.core = $routeParams.core;
      params.wt = "json";
      params.name = isSelected ? $scope.name : null;

      ParamSet.get(params, callback, failure);

      ///////

      function callback(success) {
        $scope.responseStatus = "success";
        delete success.$promise;
        delete success.$resolved;
        $scope.response = JSON.stringify(success, null, '  ');
        if (isSelected) {
          $scope.selectedParamsetList = success.response.params ? Object.keys(success.response.params) : [];
        } else {
          $scope.paramsetList = success.response.params ? Object.keys(success.response.params) : [];
        }
      }

      function failure (failure) {
        $scope.responseStatus = failure;
      }
    }
    $scope.getParamsets();
    $scope.refresh = function () {
      $scope.paramsetContent = "";
      $scope.placeholder = {
        "set": {
          "myQueries": {
            "defType": "edismax",
            "rows": "5",
            "df": "text_all"
          }
        }
      }
    };
    $scope.refresh();

    $scope.submit = function () {
      var params = {};

      params.core = $routeParams.core;
      params.wt = "json";

      ParamSet.submit(params, $scope.paramsetContent, callback, failure);

      ///////
      function callback(success) {
        $scope.responseStatus = "success";
        delete success.$promise;
        delete success.$resolved;
        $scope.response = JSON.stringify(success, null, '  ');
        $scope.getParamsets();
      }
      function failure (failure) {
        $scope.responseStatus = failure;
      }
    }
  });
