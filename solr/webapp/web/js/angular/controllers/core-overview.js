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

// follower.leaderDetails comes from an internal v1 call to the leader, so its booleans are
// always the stringified "true"/"false" form, even though the rest of this response is v2.
// TODO: drop this once that internal leader fetch is v2-native too.
var isTruthy = function(value) {
    return value === true || value === 'true';
};

solrAdminApp.controller('CoreOverviewController',
function($scope, $rootScope, $routeParams, $timeout, Luke, CoreInfo, Update, ReplicationV2, Constants, ApiErrorHandler) {
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
    ReplicationV2.fetchReplicationDetails($routeParams.core, {}, function(error, data, response) {
      $timeout(function() {
        if (error) {
          // An auth failure isn't about replication being unconfigured, so route it through the
          // shared login-redirect/permission-banner handling instead of this message.
          if (response && (response.status === 401 || response.status === 403)) {
            $scope.replicationMessage = null;
            ApiErrorHandler.handle(response);
          } else {
            $scope.replicationMessage = "Replication is not configured";
          }
          return;
        }
        $scope.isFollower = isTruthy(data.details.isFollower);
        $scope.isLeader = isTruthy(data.details.isLeader);
        $scope.replication = data.details;
      });
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

  $scope.refresh = function() {
    $scope.refreshIndex();
    $scope.refreshReplication();
    $scope.refreshInfo();
  };

  $scope.refresh();
});
