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

        // Parse HELP comments
        if (line.indexOf('# HELP ') === 0) {
          var helpParts = line.substring(7).split(' ');
          currentMetricName = helpParts[0];
          currentMetricHelp = helpParts.slice(1).join(' ');
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
            // Handle histogram suffixes (_sum, _count, _bucket)
            var baseMetricName = sample.metricName.replace(/_sum$|_count$|_bucket$/, '');

            if (!metrics[baseMetricName]) {
              metrics[baseMetricName] = {
                type: 'unknown',
                help: '',
                samples: []
              };
            }

            // Add suffix info to sample
            if (sample.metricName.indexOf('_sum') === sample.metricName.length - 4) {
              sample.metricSuffix = '_sum';
            } else if (sample.metricName.indexOf('_count') === sample.metricName.length - 6) {
              sample.metricSuffix = '_count';
            } else if (sample.metricName.indexOf('_bucket') === sample.metricName.length - 7) {
              sample.metricSuffix = '_bucket';
            }

            metrics[baseMetricName].samples.push(sample);
          }
        }
      }

      return metrics;
    }

    /**
     * Parse a single metric line
     * @param {string} line - Metric line (e.g., 'metric_name{label1="val1"} 123.45')
     * @returns {Object|null} Parsed sample or null
     */
    function parseMetricLine(line) {
      // Regex to match: metric_name{labels} value
      // or: metric_name value
      var match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*?)(?:\{(.*?)\})?\s+([^\s]+)$/);

      if (!match) return null;

      var metricName = match[1];
      var labelsStr = match[2] || '';
      var value = parseFloat(match[3]);

      // Parse labels
      var labels = {};
      if (labelsStr) {
        // Match label="value" patterns
        var labelRegex = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;
        var labelMatch;
        while ((labelMatch = labelRegex.exec(labelsStr)) !== null) {
          // Unescape label values
          var labelValue = labelMatch[2].replace(/\\"/g, '"').replace(/\\\\/g, '\\');
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
