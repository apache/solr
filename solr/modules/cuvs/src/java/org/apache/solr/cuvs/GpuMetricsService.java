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
package org.apache.solr.cuvs;

import com.codahale.metrics.Gauge;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricManager.ResolutionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service that collects GPU metrics for the Solr admin interface. */
public class GpuMetricsService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static GpuMetricsService instance;
  private SolrMetricManager metricManager;
  private ScheduledExecutorService scheduler;
  private final AtomicLong gpuCount = new AtomicLong(0);
  private final AtomicLong gpuMemoryTotal = new AtomicLong(0);
  private final AtomicLong gpuMemoryUsed = new AtomicLong(0);
  private final AtomicLong gpuMemoryFree = new AtomicLong(0);
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean staticDataInitialized = new AtomicBoolean(false);
  private final AtomicReference<ConcurrentHashMap<String, Object>> gpuDevices =
      new AtomicReference<>(new ConcurrentHashMap<>());

  private GpuMetricsService() {}

  public static synchronized GpuMetricsService getInstance() {
    if (instance == null) {
      instance = new GpuMetricsService();
    }
    return instance;
  }

  public void initialize(SolrCore core) {
    if (initialized.compareAndSet(false, true)) {
      this.metricManager = core.getCoreContainer().getMetricManager();
      registerMetrics();
      startBackgroundService();
      log.info("GPU metrics service initialized");
    }
  }

  private void registerMetrics() {
    metricManager.registerGauge(
        null,
        "solr.node",
        (Gauge<Long>) () -> gpuCount.get(),
        "gpu-metrics",
        ResolutionStrategy.REPLACE,
        "gpu.count");
    metricManager.registerGauge(
        null,
        "solr.node",
        (Gauge<Long>) () -> gpuMemoryTotal.get(),
        "gpu-metrics",
        ResolutionStrategy.REPLACE,
        "gpu.memory.total");
    metricManager.registerGauge(
        null,
        "solr.node",
        (Gauge<Long>) () -> gpuMemoryUsed.get(),
        "gpu-metrics",
        ResolutionStrategy.REPLACE,
        "gpu.memory.used");
    metricManager.registerGauge(
        null,
        "solr.node",
        (Gauge<Long>) () -> gpuMemoryFree.get(),
        "gpu-metrics",
        ResolutionStrategy.REPLACE,
        "gpu.memory.free");
    metricManager.registerGauge(
        null,
        "solr.node",
        (Gauge<ConcurrentHashMap<String, Object>>) () -> gpuDevices.get(),
        "gpu-metrics",
        ResolutionStrategy.REPLACE,
        "gpu.devices");
  }

  private void startBackgroundService() {
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "gpu-metrics-collector");
              t.setDaemon(true);
              return t;
            });

    running.set(true);
    scheduler.scheduleWithFixedDelay(this::updateGpuMetrics, 0, 5, TimeUnit.SECONDS);
    log.info("GPU metrics background service started");
  }

  private void updateGpuMetrics() {
    if (!running.get()) {
      return;
    }

    try {
      GPUInfoProvider gpuInfoProvider = CuVSProvider.provider().gpuInfoProvider();
      List<GPUInfo> gpuObjects = gpuInfoProvider.availableGPUs();

      gpuCount.set(gpuObjects.size());

      if (!gpuObjects.isEmpty()) {
        // Initialize static data only once
        if (!staticDataInitialized.get()) {
          updateStaticDeviceInfo(gpuObjects);
          staticDataInitialized.set(true);
        }
        // Update dynamic memory metrics every time
        updateDynamicMemoryMetrics(gpuInfoProvider);
      } else {
        resetDataMetrics();
        staticDataInitialized.set(false);
      }

    } catch (Exception e) {
      log.warn("Failed to update GPU metrics", e);
      resetMetrics();
    }
  }

  private void resetMetrics() {
    gpuCount.set(0);
    staticDataInitialized.set(false);
    resetDataMetrics();
  }

  private void resetDataMetrics() {
    gpuMemoryTotal.set(0);
    gpuMemoryUsed.set(0);
    gpuMemoryFree.set(0);
    gpuDevices.set(new ConcurrentHashMap<>());
  }

  private void updateDynamicMemoryMetrics(GPUInfoProvider gpuInfoProvider) {
    try {
      CuVSResources resources = CuVSResources.create();
      try {
        // Get current memory usage (only dynamic data)
        CuVSResourcesInfo currentInfo = gpuInfoProvider.getCurrentInfo(resources);
        if (currentInfo != null) {
          long free = currentInfo.freeDeviceMemoryInBytes();
          long used = gpuMemoryTotal.get() - free; // Use already stored total

          gpuMemoryUsed.set(used);
          gpuMemoryFree.set(free);
        } else {
          gpuMemoryUsed.set(0);
          gpuMemoryFree.set(gpuMemoryTotal.get());
        }
      } finally {
        resources.close();
      }
    } catch (Throwable e) {
      log.warn("Failed to update dynamic memory metrics", e);
    }
  }

  private void updateStaticDeviceInfo(List<GPUInfo> gpuObjects) {
    try {
      ConcurrentHashMap<String, Object> devices = new ConcurrentHashMap<>();
      long totalMemoryAllGpus = 0;

      for (GPUInfo gpuInfo : gpuObjects) {
        ConcurrentHashMap<String, Object> gpuDetails = new ConcurrentHashMap<>();

        // Static properties that don't change
        int gpuId = gpuInfo.gpuId();
        String name = gpuInfo.name();
        long totalMemory = gpuInfo.totalDeviceMemoryInBytes();
        int computeMajor = gpuInfo.computeCapabilityMajor();
        int computeMinor = gpuInfo.computeCapabilityMinor();
        boolean concurrentCopy = gpuInfo.supportsConcurrentCopy();
        boolean concurrentKernels = gpuInfo.supportsConcurrentKernels();

        gpuDetails.put("id", gpuId);
        gpuDetails.put("name", name);
        gpuDetails.put("totalMemory", totalMemory);
        gpuDetails.put("computeCapability", computeMajor + "." + computeMinor);
        gpuDetails.put("supportsConcurrentCopy", concurrentCopy);
        gpuDetails.put("supportsConcurrentKernels", concurrentKernels);

        devices.put("gpu_" + gpuId, gpuDetails);
        totalMemoryAllGpus += totalMemory;
      }

      // Set static data only once
      gpuDevices.set(devices);
      gpuMemoryTotal.set(totalMemoryAllGpus);

    } catch (Exception e) {
      log.warn("Failed to update static device info", e);
    }
  }

  public void shutdown() {
    running.set(false);
    if (scheduler != null) {
      scheduler.shutdownNow();
      log.info("GPU metrics service shut down");
    }
  }
}
