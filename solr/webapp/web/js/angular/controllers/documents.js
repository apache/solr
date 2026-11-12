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

var DOC_PLACEHOLDER = '<doc>\n' +
                '  <field name="id">change.me</field>\n' +
                '  <field name="title">change.me</field>\n' +
                '</doc>';

var JSON_DOC_PLACEHOLDER = '{\n' +
                '  "id": "change.me",\n' +
                '  "title": "change.me"\n' +
                '}';
var JSON_COMMAND_PLACEHOLDER = '{\n' +
                '  "add": {\n' +
                '    "doc": {\n' +
                '      "id": "change.me",\n' +
                '      "title": "change.me"\n' +
                '    }\n' +
                '  }\n' +
                '}';

solrAdminApp.controller('DocumentsController',
    function($scope, $routeParams, Luke, UpdateV2, FileUpload, Constants, ApiErrorHandler) {
        $scope.resetMenu("documents", Constants.IS_COLLECTION_PAGE);

        $scope.refresh = function () {
            Luke.schema({core: $routeParams.core}, function(data) {
                //TODO: handle dynamic fields
                delete data.schema.fields._version_;
                $scope.fields = Object.keys(data.schema.fields);
            });
            $scope.document = "";
            $scope.type = "json";
            $scope.commitWithin = 1000;
            $scope.overwrite = true;
        };

        $scope.refresh();

        $scope.changeDocumentType = function () {
            $scope.placeholder = "";
            if ($scope.type == 'json') {
                $scope.placeholder = JSON_DOC_PLACEHOLDER;
            } else if ($scope.type == 'csv') {
                $scope.placeholder = "id,title\nchange.me,change.me";
            } else if ($scope.type == 'solr-json') {
                $scope.placeholder = JSON_COMMAND_PLACEHOLDER;
            } else if ($scope.type == 'xml') {
                $scope.placeholder = DOC_PLACEHOLDER;
            }
        };

        $scope.addWizardField = function () {
            if ($scope.document == "") $scope.document = "{}";
            var doc = JSON.parse($scope.document);
            doc[$scope.fieldName] = $scope.fieldData;
            $scope.document = JSON.stringify(doc, null, '\t');
            $scope.fieldData = "";
        };

        $scope.submit = function () {
            if ($scope.type == "upload") {
                FileUpload.upload({
                    core: $routeParams.core,
                    handler: "update",
                    commitWithin: $scope.commitWithin,
                    overwrite: $scope.overwrite,
                    wt: "json",
                    raw: $scope.literalParams
                }, $scope.fileUpload, function (data) {
                    $scope.responseStatus = "success";
                    $scope.response = JSON.stringify(data, null, '  ');
                }, function (data) {
                    $scope.responseStatus = "failure";
                    $scope.response = JSON.stringify(data, null, '  ');
                });
                return;
            }

            var postData;
            var updateMethod;
            if ($scope.type == "json" || $scope.type == "wizard") {
                postData = "[" + $scope.document + "]";
                updateMethod = UpdateV2.update;
            } else if ($scope.type == "solr-json") {
                postData = $scope.document;
                updateMethod = UpdateV2.update;
            } else if ($scope.type == "xml") {
                postData = "<add>" + $scope.document + "</add>";
                updateMethod = UpdateV2.updateXml;
            } else if ($scope.type == "csv") {
                postData = $scope.document;
                updateMethod = UpdateV2.updateCsv;
            }
            if (!updateMethod || $scope.isCloudEnabled === undefined) return;

            var indexType = $scope.isCloudEnabled ? "collections" : "cores";
            var updateOptions = {
                commitWithin: $scope.commitWithin,
                overwrite: $scope.overwrite
            };
            var v2Callback = function (error, data, response) {
                if (error) {
                    $scope.responseStatus = "failure";
                    $scope.response = JSON.stringify((response && response.body) || error, null, '  ');
                    ApiErrorHandler.handle(response);
                    return;
                }
                $scope.responseStatus = "success";
                $scope.response = JSON.stringify(data, null, '  ');
                $scope.$evalAsync();
            };
            updateMethod.call(UpdateV2, indexType, $routeParams.core, postData, updateOptions, v2Callback);
        }
    });
