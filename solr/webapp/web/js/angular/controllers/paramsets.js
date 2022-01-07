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

solrAdminApp.controller('ParamsetsController',
  function($scope, $rootScope, $routeParams, $location, Set, FileUpload, Constants) {
    $scope.resetMenu("paramsets", Constants.IS_COLLECTION_PAGE);

    $scope.refresh = function () {
      $scope.paramset = "";
      $scope.handler = "/set";
      $scope.type = "json";
    };

    $scope.refresh();

    $scope.submit = function () {
      /*
      var myHeaders = new Headers();
      myHeaders.append("Content-type", "application/json");

      var raw = JSON.stringify({
        "set": {
          "queryParam": {
            "facet": "true",
            "facet.limit": 5
          }
        }
      });

      var requestOptions = {
        method: 'POST',
        headers: myHeaders,
        body: raw,
        redirect: 'follow'
      };

      fetch("http://localhost:8983/solr/gettingstarted/config/params", requestOptions)
        .then(response => response.text())
        .then(result => console.log(result))
        .catch(error => console.log('error', error));
      */
      var contentType = "";
      var postData = "";
      var params = {};

      if ($scope.handler[0] == '/') {
        params.handler = $scope.handler.substring(1);
      } else {
        params.handler = 'set';
        params.qt = $scope.handler;
      }

      params.core = $routeParams.core;
      params.wt = "json";

      if ($scope.type == "json") {
        postData = "[" + $scope.paramset + "]";
        contentType = "json";
      } else {
          alert("Cannot identify content type")
      }

      var callback = function (success) {
        $scope.responseStatus = "success";
        delete success.$promise;
        delete success.$resolved;
        $scope.response = JSON.stringify(success, null, '  ');
      };
      var failure = function (failure) {
        $scope.responseStatus = failure;
      };
      if (contentType == "json") {
        Set.postJson(params, postData, callback, failure);
      }
    }
  });

