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

solrAdminApp.controller('PluginsController',
    function($scope, $rootScope, $routeParams, $location, Metrics, Constants) {
        $scope.resetMenu("plugins", Constants.IS_CORE_PAGE);

        if ($routeParams.legacytype) {
            // support legacy URLs. Angular cannot change #path without reloading controller
            $location.path("/"+$routeParams.core+"/plugins");
            $location.search("type", $routeParams.legacytype);
            return;
        }

        $scope.refresh = function() {
            var params = {};
            if ($routeParams.core) {
                params.core = $routeParams.core;
            }

            var type = $location.search().type;

            Metrics.raw(params, function (response) {
                $scope.types = getPluginTypesFromMetrics(response.data, type);
                $scope.type = getSelectedType($scope.types, type);

                if ($scope.type && $routeParams.entry) {
                    $scope.plugins = $routeParams.entry.split(",");
                    openPlugins($scope.type, $scope.plugins);
                } else {
                    $scope.plugins = [];
                }
            });
        };

        $scope.selectPluginType = function(type) {
            $location.search({entry:null, type: type.lower});
            $scope.type = type;
        };

        $scope.selectPlugin = function(plugin) {
            plugin.open = !plugin.open;

            if (plugin.open) {
                $scope.plugins.push(plugin.name);
            } else {
                $scope.plugins.splice($scope.plugins.indexOf(plugin.name), 1);
            }

            if ($scope.plugins.length==0) {
                $location.search("entry", null);
            } else {
                $location.search("entry", $scope.plugins.join(','));
            }
        }

        $scope.refresh();
    });

var getPluginTypesFromMetrics = function(metricsText, selected) {
    var keys = [];

    // Parse Prometheus format metrics
    var lines = metricsText.split('\n');
    var categoriesMap = {};
    var metricMetadata = {}; // Store HELP and TYPE info for each metric

    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();

        // Skip empty lines
        if (line === '') {
            continue;
        }

        // Parse HELP comments - format: # HELP metric_name description
        if (line.startsWith('# HELP ')) {
            var helpMatch = line.match(/^# HELP\s+([^\s]+)\s+(.*)$/);
            if (helpMatch) {
                var metricName = helpMatch[1];
                var description = helpMatch[2];
                if (!metricMetadata[metricName]) {
                    metricMetadata[metricName] = {};
                }
                metricMetadata[metricName].description = description;
            }
            continue;
        }

        // Parse TYPE comments - format: # TYPE metric_name type
        if (line.startsWith('# TYPE ')) {
            var typeMatch = line.match(/^# TYPE\s+([^\s]+)\s+(.*)$/);
            if (typeMatch) {
                var metricName = typeMatch[1];
                var type = typeMatch[2];
                if (!metricMetadata[metricName]) {
                    metricMetadata[metricName] = {};
                }
                metricMetadata[metricName].type = type;
            }
            continue;
        }

        // Skip other comments
        if (line.startsWith('#')) {
            continue;
        }

        // Parse metric line - format: metric_name{labels} value timestamp
        var metricMatch = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+([^\s]+)(\s+[^\s]+)?$/);
        if (!metricMatch) {
            // Try without labels - format: metric_name value timestamp
            metricMatch = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+([^\s]+)(\s+[^\s]+)?$/);
            if (metricMatch) {
                // Skip metrics without category labels for prometheus format
                continue;
            }
            continue;
        }

        var metricName = metricMatch[1];
        var labelsStr = metricMatch[2];
        var value = metricMatch[3];

        // Parse labels
        var labels = {};
        if (labelsStr) {
            var labelPairs = labelsStr.split(',');
            for (var j = 0; j < labelPairs.length; j++) {
                var labelMatch = labelPairs[j].trim().match(/^([^=]+)="([^"]*)"$/);
                if (labelMatch) {
                    labels[labelMatch[1]] = labelMatch[2];
                }
            }
        }

        // Use category from labels only - don't fall back to metric name parsing
        var category = labels.category;

        // Skip metrics that don't have a category label
        if (!category) {
            continue;
        }

        if (!categoriesMap[category]) {
            categoriesMap[category] = {};
        }

        if (!categoriesMap[category][metricName]) {
            categoriesMap[category][metricName] = {};
        }

        // Create a descriptive key for the metric variant
        var labelParts = [];
        for (var labelKey in labels) {
            if (labelKey !== 'category') {
                labelParts.push(labelKey + '=' + labels[labelKey]);
            }
        }
        var variantKey = labelParts.length > 0 ? labelParts.join(', ') : 'default';

        categoriesMap[category][metricName][variantKey] = value;
    }

    // Convert to the expected format
    for (var categoryName in categoriesMap) {
        var lower = categoryName.toLowerCase();
        var metrics = [];

        for (var metricName in categoriesMap[categoryName]) {
            var metricData = categoriesMap[categoryName][metricName];
            var metadata = metricMetadata[metricName] || {};
            metrics.push({
                name: metricName,
                changed: false,
                stats: metricData,
                open: false,
                properties: {
                    description: metadata.description,
                    type: metadata.type
                }
            });
        }

        if (metrics.length > 0) {
            keys.push({
                name: categoryName,
                selected: lower == selected,
                changes: 0,
                lower: lower,
                plugins: metrics
            });
        }
    }

    return keys;
};

var getSelectedType = function(types, selected) {
    if (selected) {
        for (var i in types) {
            if (types[i].lower == selected) {
                return types[i];
            }
        }
    }
};

var openPlugins = function(type, selected) {
    for (var i in type.plugins) {
        var plugin = type.plugins[i];
        plugin.open = selected.indexOf(plugin.name)>=0;
    }
}
