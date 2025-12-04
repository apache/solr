/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Prometheus text format parser for Solr Admin UI
 *
 * Parses Prometheus exposition format (text-based format for metrics)
 * into a structured JavaScript object for consumption by the Admin UI.
 */

(function() {
  'use strict';

  angular.module('solrAdminApp').factory('PrometheusParser', function() {

    /**
     * Parse Prometheus text format into structured JavaScript object
     * @param {string} prometheusText - Raw Prometheus format text
     * @returns {Object} Parsed metrics object keyed by metric name
     */
    function parsePrometheusFormat(prometheusText) {
      if (!prometheusText || typeof prometheusText !== 'string') {
        return {};
      }

      var metrics = {};
      var lines = prometheusText.split('\n');
      var currentMetricName = null;
      var currentMetricType = null;
      var currentMetricHelp = null;

      for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();

        // Skip empty lines
        if (!line) continue;

        // Parse HELP comments - use regex for robust parsing
        if (line.indexOf('# HELP ') === 0) {
          var helpMatch = line.match(/^# HELP ([a-zA-Z_:][a-zA-Z0-9_:]*)\s+(.*)$/);
          if (helpMatch) {
            currentMetricName = helpMatch[1];
            currentMetricHelp = helpMatch[2];
          }
        }
        // Parse TYPE comments
        else if (line.indexOf('# TYPE ') === 0) {
          var typeParts = line.substring(7).split(' ');
          currentMetricName = typeParts[0];
          currentMetricType = typeParts[1];

          // Initialize metric entry
          if (!metrics[currentMetricName]) {
            metrics[currentMetricName] = {
              type: currentMetricType,
              help: currentMetricHelp || '',
              samples: []
            };
          }
        }
        // Skip other comments
        else if (line.charAt(0) === '#') {
          continue;
        }
        // Parse metric sample
        else {
          var sample = parseMetricLine(line);
          if (sample && sample.metricName) {
            var baseMetricName = sample.metricName;
            var metricSuffix = null;

            // Only strip suffixes for histogram and summary types
            // Check if metric name has known suffixes
            if (sample.metricName.indexOf('_sum') === sample.metricName.length - 4) {
              baseMetricName = sample.metricName.substring(0, sample.metricName.length - 4);
              metricSuffix = '_sum';
            } else if (sample.metricName.indexOf('_count') === sample.metricName.length - 6) {
              baseMetricName = sample.metricName.substring(0, sample.metricName.length - 6);
              metricSuffix = '_count';
            } else if (sample.metricName.indexOf('_bucket') === sample.metricName.length - 7) {
              baseMetricName = sample.metricName.substring(0, sample.metricName.length - 7);
              metricSuffix = '_bucket';
            } else if (sample.metricName.indexOf('_total') === sample.metricName.length - 6) {
              // Handle _total suffix for summary metrics
              baseMetricName = sample.metricName.substring(0, sample.metricName.length - 6);
              metricSuffix = '_total';
            }

            // Check if base metric exists with histogram/summary type
            var shouldGroup = false;
            if (metricSuffix && metrics[baseMetricName]) {
              var baseType = metrics[baseMetricName].type;
              shouldGroup = (baseType === 'histogram' || baseType === 'summary');
            }

            // Use base name if we should group, otherwise use full name
            var targetMetricName = (shouldGroup || metricSuffix) ? baseMetricName : sample.metricName;

            if (!metrics[targetMetricName]) {
              metrics[targetMetricName] = {
                type: currentMetricType || 'unknown',
                help: currentMetricHelp || '',
                samples: []
              };
            }

            // Add suffix info to sample if present
            if (metricSuffix) {
              sample.metricSuffix = metricSuffix;
            }

            metrics[targetMetricName].samples.push(sample);
          }
        }
      }

      return metrics;
    }

    /**
     * Parse a single metric line
     * @param {string} line - Metric line (e.g., 'metric_name{label1="val1"} 123.45' or with timestamp)
     * @returns {Object|null} Parsed sample or null
     */
    function parseMetricLine(line) {
      // Regex to match: metric_name{labels} value [timestamp]
      // or: metric_name value [timestamp]
      // The timestamp is optional and is a Unix timestamp in milliseconds
      // The value pattern [^\s]+ matches scientific notation (e.g., 1.23e-4) and special values (NaN, +Inf, -Inf).
      // The label set is matched as everything between { and }, allowing for escaped braces inside quoted label values.
      var match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{((?:"(?:[^"\\]|\\.)*"|[^}])*)\})?\s+([^\s]+)(?:\s+\d+)?$/);

      if (!match) return null;

      var metricName = match[1];
      var labelsStr = match[2] || '';
      var value = parseFloat(match[3]);

      // Parse labels
      var labels = {};
      if (labelsStr) {
        // Match label="value" patterns - only allow valid Prometheus escape sequences (\\, \", \n)
        var labelRegex = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\[\\"n])*)"/g;
        var labelMatch;
        while ((labelMatch = labelRegex.exec(labelsStr)) !== null) {
          // Unescape label values - must unescape \\ first to avoid double-unescaping
          var labelValue = labelMatch[2].replace(/\\\\/g, '\\').replace(/\\"/g, '"').replace(/\\n/g, '\n');
          labels[labelMatch[1]] = labelValue;
        }
      }

      return {
        metricName: metricName,
        labels: labels,
        value: value
      };
    }

    // Export public API
    return {
      parse: parsePrometheusFormat
    };
  });
})();
