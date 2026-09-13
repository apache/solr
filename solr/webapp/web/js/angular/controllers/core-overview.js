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

solrAdminApp.controller('CoreOverviewController',
function($scope, $rootScope, $routeParams, Luke, CoreInfo, Update, Replication, Ping, Constants) {
  $scope.resetMenu("overview", Constants.IS_CORE_PAGE);
  $scope.refreshIndex = function() {
    Luke.index({core: $routeParams.core},
      function(data) {
        $scope.index = data.index;
        delete $scope.statsMessage;
      },
      function(error) {
        $scope.statsMessage = "Luke is not configured";
      }
    );
  };

  $scope.refreshReplication = function() {
    Replication.details({core: $routeParams.core},
      function(data) {
        $scope.isFollower = data.details.isFollower == "true";
        $scope.isLeader = data.details.isLeader == "true";
        $scope.replication = data.details;
      },
      function(error) {
        $scope.replicationMessage = "Replication is not configured";
      });
  };

  $scope.refreshInfo = function() {
    CoreInfo.get({core: $routeParams.core},
      function(data) {
        $scope.core = data.core;
        delete $scope.systemMessage;
      },
      function(error) {
        $scope.systemMessage = "/admin/system Handler is not configured";
      }
    );
  };

  $scope.refreshPing = function() {
    Ping.status({core: $routeParams.core}, function(data) {
      // Three states, and they are not interchangeable. "enabled" and "disabled" both mean a
      // healthcheck file is configured, so toggleHealthcheck() works and the widget shows the
      // lit / unlit control. "not_configured" means there is no healthcheck file at all, and
      // enable/disable would answer 503 - so set a message, which hides the toggle rather than
      // offering a control that cannot work.
      delete $scope.healthcheckMessage;
      if (data.status == "enabled" || data.status == "disabled") {
        $scope.healthcheckStatus = data.status == "enabled";
      } else {
        $scope.healthcheckStatus = false;
        $scope.healthcheckMessage = data.status == "not_configured"
          ? 'Ping request handler is not configured with a healthcheck file.'
          : 'Unexpected ping status: ' + data.status;
      }
    }, function(error) {
      $scope.healthcheckStatus = false;
      $scope.healthcheckMessage = error.data && error.data.error ? error.data.error.msg : 'Unable to read ping status.';
    });
  };

  $scope.toggleHealthcheck = function() {
    if ($scope.healthcheckStatus) {
      Ping.disable(
        {core: $routeParams.core},
        function(data) {$scope.healthcheckStatus = false},
        function(error) {$scope.healthcheckMessage = error}
      );
    } else {
      Ping.enable(
        {core: $routeParams.core},
        function(data) {$scope.healthcheckStatus = true},
        function(error) {$scope.healthcheckMessage = error}
      );
    }
  };

  $scope.refresh = function() {
    $scope.refreshIndex();
    $scope.refreshReplication();
    $scope.refreshInfo();
    $scope.refreshPing();
  };

  $scope.refresh();
});
