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

solrAdminApp.controller('ThreadsController',
  function($scope, $timeout, ThreadsV2, Constants, ApiErrorHandler){
    $scope.resetMenu("threads", Constants.IS_ROOT_PAGE);
    $scope.refresh = function() {
      ThreadsV2.getThreadDump(function(error, data, response) {
        $timeout(function() {
          if (error) { ApiErrorHandler.handle(response); return; }
          if (!data || !data.system) {
            $scope.threads = [];
            return;
          }
          var threadDump = data.system.threadDump;
          var threads = [];
          for (var i=0; i<threadDump.length; i++) {
            var thread = threadDump[i].thread;
            if (!!thread.stackTrace) {
              var stackTrace = [];
              for (var j=0; j<thread.stackTrace.length; j++) {
                var trace = thread.stackTrace[j].replace("(", "\u200B("); // allow wrapping to happen, \u200B is a zero-width space
                stackTrace.push({id:thread.id + ":" + j, trace: trace});
              }
              thread.stackTrace = stackTrace;
            }
            threads.push(thread);
          }
          $scope.threads = threads;
        });
      });
    };
    $scope.toggleStacktrace = function(thread) {
      thread.showStackTrace = !thread.showStackTrace;
    };
    $scope.toggleStacktraces = function() {
      $scope.showAllStacktraces = !$scope.showAllStacktraces;
      for (var i=0; i<$scope.threads.length; i++) {
        $scope.threads[i].showStackTrace = $scope.showAllStacktraces;
      }
    };
    $scope.refresh();
});
