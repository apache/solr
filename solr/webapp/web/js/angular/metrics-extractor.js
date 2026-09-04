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
 * Metrics extraction helper for Solr Admin UI
 *
 * Provides helper functions to extract specific metric values from
 * parsed Prometheus metrics data.
 */

(function() {
  'use strict';

  angular.module('solrAdminApp').factory('MetricsExtractor', function() {

    /**
     * Find a metric sample by label filters
     * @param {Object} metric - Parsed metric object with samples array
     * @param {Object} labelFilters - Object with label key-value pairs to match
     * @returns {Object|null} Matching sample or null
     */
    function findSample(metric, labelFilters) {
      if (!metric || !metric.samples) return null;

      for (var i = 0; i < metric.samples.length; i++) {
        var sample = metric.samples[i];
        var matches = true;

        for (var key in labelFilters) {
          if (labelFilters.hasOwnProperty(key)) {
            if (!sample.labels || sample.labels[key] !== labelFilters[key]) {
              matches = false;
              break;
            }
          }
        }

        if (matches) {
          return sample;
        }
      }

      return null;
    }

    /**
     * Extract disk metrics (total and usable space)
     * @param {Object} parsedMetrics - Parsed Prometheus metrics
     * @param {Object} labelFilters - Optional additional label filters (e.g., {node: "nodeName"})
     * @returns {Object} Object with totalSpace and usableSpace in bytes
     */
    function extractDiskMetrics(parsedMetrics, labelFilters) {
      var diskMetric = parsedMetrics['solr_disk_space_megabytes'];
      if (!diskMetric) {
        return { totalSpace: 0, usableSpace: 0 };
      }

      // Merge standard filters with optional filters
      var totalFilters = { category: 'CONTAINER', type: 'total_space' };
      var usableFilters = { category: 'CONTAINER', type: 'usable_space' };

      if (labelFilters) {
        for (var key in labelFilters) {
          if (labelFilters.hasOwnProperty(key)) {
            totalFilters[key] = labelFilters[key];
            usableFilters[key] = labelFilters[key];
          }
        }
      }

      var totalSample = findSample(diskMetric, totalFilters);
      var usableSample = findSample(diskMetric, usableFilters);

      return {
        totalSpace: totalSample ? totalSample.value * 1024 * 1024 : 0,  // MB to bytes
        usableSpace: usableSample ? usableSample.value * 1024 * 1024 : 0  // MB to bytes
      };
    }

    /**
     * Extract core index size
     * @param {Object} parsedMetrics - Parsed Prometheus metrics
     * @param {Object} coreLabels - Labels to identify the core (must include 'core')
     * @returns {number} Index size in bytes
     */
    function extractCoreIndexSize(parsedMetrics, coreLabels) {
      var indexSizeMetric = parsedMetrics['solr_core_index_size_megabytes'];
      if (!indexSizeMetric) return 0;

      var sample = findSample(indexSizeMetric, coreLabels);
      return sample ? sample.value * 1024 * 1024 : 0;  // MB to bytes
    }

    /**
     * Extract searcher metrics (numDocs, deletedDocs, warmupTime)
     * @param {Object} parsedMetrics - Parsed Prometheus metrics
     * @param {Object} coreLabels - Labels to identify the core (must include 'core')
     * @returns {Object} Object with numDocs, deletedDocs, warmupTime
     */
    function extractSearcherMetrics(parsedMetrics, coreLabels) {
      var numDocsMetric = parsedMetrics['solr_core_indexsearcher_index_num_docs'];
      var totalDocsMetric = parsedMetrics['solr_core_indexsearcher_index_docs'];
      var openTimeMetric = parsedMetrics['solr_core_indexsearcher_open_time_milliseconds'];

      var numDocsSample = findSample(numDocsMetric, coreLabels);
      var totalDocsSample = findSample(totalDocsMetric, coreLabels);

      var numDocs = numDocsSample ? numDocsSample.value : 0;
      var totalDocs = totalDocsSample ? totalDocsSample.value : 0;
      var deletedDocs = totalDocs - numDocs;

      // For warmup time, look for _sum sample from the histogram
      var warmupTime = 0;
      if (openTimeMetric) {
        for (var i = 0; i < openTimeMetric.samples.length; i++) {
          var sample = openTimeMetric.samples[i];

          // Check if labels match
          var labelsMatch = true;
          for (var key in coreLabels) {
            if (coreLabels.hasOwnProperty(key)) {
              if (!sample.labels || sample.labels[key] !== coreLabels[key]) {
                labelsMatch = false;
                break;
              }
            }
          }

          // Check if this is the _sum sample
          if (labelsMatch && sample.metricSuffix === '_sum') {
            warmupTime = sample.value;
            break;
          }
        }
      }

      return {
        numDocs: numDocs,
        deletedDocs: deletedDocs,
        warmupTime: warmupTime
      };
    }

    // Export public API
    return {
      extractDiskMetrics: extractDiskMetrics,
      extractCoreIndexSize: extractCoreIndexSize,
      extractSearcherMetrics: extractSearcherMetrics,
      findSample: findSample
    };
  });
})();
