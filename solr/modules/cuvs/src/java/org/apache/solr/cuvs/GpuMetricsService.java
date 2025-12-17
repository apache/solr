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

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.CuVSResourcesInfo;
import com.nvidia.cuvs.GPUInfo;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.spi.CuVSProvider;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.GpuMetricsProvider;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GpuMetricsService implements GpuMetricsProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static GpuMetricsService instance;

  private SolrMetricManager metricManager;
  private SolrMetricsContext metricsContext;
  private ScheduledExecutorService scheduler;

  private ObservableLongGauge gpuCountGauge;
  private ObservableLongGauge gpuMemoryTotalGauge;
  private ObservableLongGauge gpuMemoryUsedGauge;
  private ObservableLongGauge gpuMemoryFreeGauge;

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

  public void initialize(CoreContainer coreContainer) {
    if (initialized.compareAndSet(false, true)) {
      this.metricManager = coreContainer.getMetricManager();
      startBackgroundService();
      log.info("GPU metrics service initialized");
    }
  }

  public void initialize(SolrCore core) {
    initialize(core.getCoreContainer());
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    this.metricsContext = parentContext;

    gpuCountGauge =
        metricManager.observableLongGauge(
            parentContext.getRegistryName(),
            "gpu.count",
            "Number of available GPUs",
            measurement -> measurement.record(gpuCount.get()),
            null);

    gpuMemoryTotalGauge =
        metricManager.observableLongGauge(
            parentContext.getRegistryName(),
            "gpu.memory.total",
            "Total GPU memory in bytes",
            measurement -> measurement.record(gpuMemoryTotal.get()),
            null);

    gpuMemoryUsedGauge =
        metricManager.observableLongGauge(
            parentContext.getRegistryName(),
            "gpu.memory.used",
            "Used GPU memory in bytes",
            measurement -> measurement.record(gpuMemoryUsed.get()),
            null);

    gpuMemoryFreeGauge =
        metricManager.observableLongGauge(
            parentContext.getRegistryName(),
            "gpu.memory.free",
            "Free GPU memory in bytes",
            measurement -> measurement.record(gpuMemoryFree.get()),
            null);

    log.info("GPU metrics registered with OpenTelemetry");
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return metricsContext;
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
      long totalUsed = 0;
      long totalFree = 0;

      ConcurrentHashMap<String, Object> currentDevices = gpuDevices.get();

      CuVSResources resources = CuVSResources.create();
      try {
        CuVSResourcesInfo currentInfo = gpuInfoProvider.getCurrentInfo(resources);
        if (currentInfo != null) {
          long firstGpuFreeMemory = currentInfo.freeDeviceMemoryInBytes();

          String firstDeviceKey = null;
          ConcurrentHashMap<String, Object> firstDevice = null;
          for (String deviceKey : currentDevices.keySet()) {
            firstDeviceKey = deviceKey;
            @SuppressWarnings("unchecked")
            ConcurrentHashMap<String, Object> device =
                (ConcurrentHashMap<String, Object>) currentDevices.get(deviceKey);
            firstDevice = device;
            break;
          }

          if (firstDevice != null) {
            long firstGpuTotalMemory = (Long) firstDevice.get("totalMemory");
            long firstGpuUsedMemory = firstGpuTotalMemory - firstGpuFreeMemory;

            // Mark the first GPU as active and update with actual memory values
            firstDevice.put("usedMemory", firstGpuUsedMemory);
            firstDevice.put("freeMemory", firstGpuFreeMemory);
            firstDevice.put("active", true);

            totalUsed = firstGpuUsedMemory;
            totalFree = firstGpuFreeMemory;

            log.debug(
                "Updated active GPU {} memory: used={}, free={}, total={}",
                firstDeviceKey,
                firstGpuUsedMemory,
                firstGpuFreeMemory,
                firstGpuTotalMemory);

            // Setting other GPUs as inactive (no memory data, just names/IDs)
            for (String deviceKey : currentDevices.keySet()) {
              if (!deviceKey.equals(firstDeviceKey)) {
                @SuppressWarnings("unchecked")
                ConcurrentHashMap<String, Object> device =
                    (ConcurrentHashMap<String, Object>) currentDevices.get(deviceKey);
                device.put("active", false);
              }
            }
          }
        } else {
          String firstDeviceKey = null;
          for (String deviceKey : currentDevices.keySet()) {
            @SuppressWarnings("unchecked")
            ConcurrentHashMap<String, Object> device =
                (ConcurrentHashMap<String, Object>) currentDevices.get(deviceKey);

            if (firstDeviceKey == null) {
              // First GPU is active and gets memory data
              firstDeviceKey = deviceKey;
              long deviceTotalMemory = (Long) device.get("totalMemory");
              device.put("usedMemory", 0L);
              device.put("freeMemory", deviceTotalMemory);
              device.put("active", true);
              totalFree = deviceTotalMemory;
            } else {
              // Other GPUs are inactive
              device.put("active", false);
            }
          }
        }
      } finally {
        resources.close();
      }

      gpuMemoryUsed.set(totalUsed);
      gpuMemoryFree.set(totalFree);

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
        gpuDetails.put("usedMemory", 0L);
        gpuDetails.put("freeMemory", totalMemory);
        gpuDetails.put("active", gpuId == 0); // First GPU (ID 0) is active by default

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

  @Override
  public Map<String, Object> getGpuDevices() {
    return gpuDevices.get();
  }

  @Override
  public long getGpuCount() {
    return gpuCount.get();
  }

  @Override
  public long getGpuMemoryTotal() {
    return gpuMemoryTotal.get();
  }

  @Override
  public long getGpuMemoryUsed() {
    return gpuMemoryUsed.get();
  }

  @Override
  public long getGpuMemoryFree() {
    return gpuMemoryFree.get();
  }

  @Override
  public void close() throws IOException {
    running.set(false);

    if (gpuCountGauge != null) {
      gpuCountGauge.close();
    }
    if (gpuMemoryTotalGauge != null) {
      gpuMemoryTotalGauge.close();
    }
    if (gpuMemoryUsedGauge != null) {
      gpuMemoryUsedGauge.close();
    }
    if (gpuMemoryFreeGauge != null) {
      gpuMemoryFreeGauge.close();
    }

    if (scheduler != null) {
      scheduler.shutdownNow();
      log.info("GPU metrics service shut down");
    }

    GpuMetricsProvider.super.close();
  }

  public void shutdown() {
    try {
      close();
    } catch (IOException e) {
      log.warn("Error during shutdown", e);
    }
  }
}
