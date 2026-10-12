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
solrAdminApp.controller('SQLQueryController',
  function($scope, $routeParams, $location, Query, Constants) {

    $scope.resetMenu("sqlquery", Constants.IS_COLLECTION_PAGE);
    $scope.qt = "sql";
    $scope.doExplanation = false
    $scope.gridOptions = {
        enableSorting: false,
        enableRowHashing:false,
        enableColumnMenus:false,
        columnDefs:[],
        data:[],
        onRegisterApi: function(gridApi) {
            $scope.gridApi = gridApi;
        }
    };
    $scope.hostPortContext = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // For display only

    // The global interceptor can route errors to the success callback, so handle both here.
    $scope.showResult = function(raw) {
      $scope.lang = "json";
      $scope.sqlError = null;
      $scope.sqlModuleMissing = false;
      $scope.sqlData = [];

      var jsonData;
      try {
        jsonData = JSON.parse(raw);
      } catch (e) {
        $scope.sqlError = raw;
        return;
      }

      var docs = jsonData && jsonData['result-set'] && jsonData['result-set'].docs;
      if (!docs) {
        var err = jsonData && jsonData.error;
        if (err && err.metadata && err.metadata['root-error-class'] === 'java.lang.ClassNotFoundException'
            && err.msg && err.msg.indexOf('SQLHandler') !== -1) {
          $scope.sqlModuleMissing = true;
          $scope.sqlError = "The sql module doesn't appear to be enabled on this Solr node.";
        } else {
          $scope.sqlError = (jsonData && jsonData.message) || (err && err.msg) || raw;
        }
        return;
      }

      for (var i = 0; i < docs.length; i++) {
          var doc = docs[i]
          if(doc.hasOwnProperty("EOF")){
              if(doc.hasOwnProperty("EXCEPTION")){
                  $scope.sqlError = doc.EXCEPTION
              }
          } else {
              $scope.gridOptions.data.push(doc);
          }
      }
      // Build grid columns from the result fields.
      var fields = $scope.gridOptions.data[1];
      for (var property in fields) {
          if (fields.hasOwnProperty(property)) {
              $scope.gridOptions.columnDefs.push({"name":property, "type":{}})
          }
      }
      $scope.gridApi.core.notifyDataChange
    };

    $scope.doQuery = function() {

      var params = {};
      params.core = $routeParams.core;
      params.handler = $scope.qt;

      var stmt = $scope.stmt

      if(!stmt.toLowerCase().replace(/(\r\n|\n|\r)/gm," ").includes(' limit ')) {
        params.stmt = [stmt + " limit 10"]
      } else {
        params.stmt = [$scope.stmt]
      }

      $scope.lang = "json";
      $scope.response = null;
      $scope.gridOptions.data =[]
      $scope.gridOptions.columnDefs = []

      $scope.url = Query.url(params);

      Query.query(params, function(data) {
        $scope.showResult(data.toJSON().data);
      }, function(rejection) {
        $scope.showResult((rejection.data && rejection.data.data) || ("HTTP " + rejection.status + " " + rejection.statusText));
      });
    };
  }
);
