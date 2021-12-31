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
package org.apache.solr.schema;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.noggit.JSONParser;
import org.apache.lucene.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Exchange Rates Provider for {@link CurrencyField} and {@link CurrencyFieldType} capable of fetching &amp; 
 * parsing the freely available exchange rates from openexchangerates.org
 * </p>
 * <p>
 * Configuration Options:
 * </p>
 * <ul>
 *  <li><code>ratesFileLocation</code> - A file path or absolute URL specifying the JSON data to load (mandatory)</li>
 *  <li><code>refreshInterval</code> - How frequently (in minutes) to reload the exchange rate data (default: 1440)</li>
 *  <li><code>refreshWhileSearching</code> - controls if currency rates should be refreshed during a search request;
 *  if you set it to false, you should set up a OpenExchangeRatesOrgReloader to refresh currency rates on commit;
 *  please refer to the documentation of OpenExchangeRatesOrgReloader for more information (default: true)</li>
 * </ul>
 * <p>
 * <b>Disclaimer:</b> This data is collected from various providers and provided free of charge
 * for informational purposes only, with no guarantee whatsoever of accuracy, validity,
 * availability or fitness for any purpose; use at your own risk. Other than that - have
 * fun, and please share/watch/fork if you think data like this should be free!
 * </p>
 * @see <a href="https://openexchangerates.org/documentation">openexchangerates.org JSON Data Format</a>
 */
public class OpenExchangeRatesOrgProvider implements ExchangeRateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static final String PARAM_RATES_FILE_LOCATION                  = "ratesFileLocation";
  protected static final String PARAM_REFRESH_INTERVAL                     = "refreshInterval";
  protected static final String PARAM_REFRESH_WHILE_SEARCHING              = "refreshWhileSearching";
  protected static final boolean PARAM_REFRESH_WHILE_SEARCHING_DEFAULT_VAL = true;
  protected static final String DEFAULT_REFRESH_INTERVAL                   = "1440";
  
  protected String ratesFileLocation;
  // configured in minutes, but stored in seconds for quicker math
  protected int refreshIntervalSeconds;
  protected boolean refreshWhileSearching = PARAM_REFRESH_WHILE_SEARCHING_DEFAULT_VAL;
  protected ResourceLoader resourceLoader;
  
  protected OpenExchangeRates rates;

  /**
   * Returns the currently known exchange rate between two currencies. The rates are fetched from
   * the freely available OpenExchangeRates.org JSON, hourly updated. All rates are symmetrical with
   * base currency being USD by default.
   *
   * @param sourceCurrencyCode The source currency being converted from.
   * @param targetCurrencyCode The target currency being converted to.
   * @return The exchange rate.
   * @throws SolrException if the requested currency pair cannot be found
   */
  @Override
  public double getExchangeRate(String sourceCurrencyCode, String targetCurrencyCode) {
    if (rates == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Rates not initialized.");
    }
      
    if (sourceCurrencyCode == null || targetCurrencyCode == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot get exchange rate; currency was null.");
    }

    if (refreshWhileSearching) {
      reloadIfExpired();
    }

    Double source = rates.getRates().get(sourceCurrencyCode);
    Double target = rates.getRates().get(targetCurrencyCode);

    if (source == null || target == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "No available conversion rate from " + sourceCurrencyCode + " to " + targetCurrencyCode + ". "
          + "Available rates are "+listAvailableCurrencies());
    }
    
    return target / source;
  }

  @SuppressForbidden(reason = "Need currentTimeMillis, for comparison with stamp in an external file")
  public void reloadIfExpired() {
    if ((rates.getTimestamp() + refreshIntervalSeconds) * 1000 < System.currentTimeMillis()) {
      log.debug("Refresh interval has expired. Refreshing exchange rates.");
      reload();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OpenExchangeRatesOrgProvider that = (OpenExchangeRatesOrgProvider) o;

    return !(rates != null ? !rates.equals(that.rates) : that.rates != null);
  }

  @Override
  public int hashCode() {
    return rates != null ? rates.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "["+this.getClass().getName()+" : " + rates.getRates().size() + " rates.]";
  }

  @Override
  public Set<String> listAvailableCurrencies() {
    if (rates == null)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Rates not initialized");
    return rates.getRates().keySet();
  }

  @Override
  @SuppressWarnings("resource")
  public boolean reload() throws SolrException {
    InputStream ratesJsonStream = null;
    try {
      log.debug("Reloading exchange rates from {}", ratesFileLocation);
      try {
        ratesJsonStream = (new URL(ratesFileLocation)).openStream();
      } catch (Exception e) {
        ratesJsonStream = resourceLoader.openResource(ratesFileLocation);
      }
        
      rates = new OpenExchangeRates(ratesJsonStream);
      return true;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reloading exchange rates", e);
    } finally {
      if (ratesJsonStream != null) {
        try {
          ratesJsonStream.close();
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Error closing stream", e);
        }
      }
    }
  }

  @Override
  public void init(Map<String,String> params) throws SolrException {
    try {
      ratesFileLocation = params.get(PARAM_RATES_FILE_LOCATION);
      if (null == ratesFileLocation) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Init param must be specified: " + PARAM_RATES_FILE_LOCATION);
      }
      int refreshInterval = Integer.parseInt(getParam(params.get(PARAM_REFRESH_INTERVAL), DEFAULT_REFRESH_INTERVAL));
      // Force a refresh interval of minimum one hour, since the API does not offer better resolution
      if (refreshInterval < 60) {
        refreshInterval = 60;
        log.warn("Specified refreshInterval was too small. Setting to 60 minutes which is the update rate of openexchangerates.org");
      }
      refreshWhileSearching = getParam(params.get(PARAM_REFRESH_WHILE_SEARCHING), PARAM_REFRESH_WHILE_SEARCHING_DEFAULT_VAL);
      log.debug("Initialized with rates={}, refreshInterval={}, refreshWhileSearching={}.", ratesFileLocation, refreshInterval, refreshWhileSearching);
      refreshIntervalSeconds = refreshInterval * 60;
    } catch (SolrException e1) {
      throw e1;
    } catch (Exception e2) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error initializing: " + 
                              e2.getMessage(), e2);
    } finally {
      // Removing config params custom to us
      params.remove(PARAM_RATES_FILE_LOCATION);
      params.remove(PARAM_REFRESH_INTERVAL);
      params.remove(PARAM_REFRESH_WHILE_SEARCHING);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws SolrException {
    resourceLoader = loader;
    reload();
  }

  private String getParam(String param, String defaultParam) {
    return param == null ? defaultParam : param;
  }

  /** Returns the boolean value of the param, or def if not set */
  private boolean getParam(String param, boolean defaultParam) {
    return param == null ? defaultParam : StrUtils.parseBool(param);
  }

  /**
   * A simple class encapsulating the JSON data from openexchangerates.org
   */
  static class OpenExchangeRates {
    private Map<String, Double> rates;
    private String baseCurrency;
    private long timestamp;
    private String disclaimer;
    private String license;
    private JSONParser parser;
    
    public OpenExchangeRates(InputStream ratesStream) throws IOException {
      parser = new JSONParser(new InputStreamReader(ratesStream, StandardCharsets.UTF_8));
      rates = new HashMap<>();
      
      int ev;
      do {
        ev = parser.nextEvent();
        switch( ev ) {
          case JSONParser.STRING:
            if( parser.wasKey() ) {
              String key = parser.getString();
              if(key.equals("disclaimer")) {
                parser.nextEvent();
                disclaimer = parser.getString();
              } else if(key.equals("license")) {
                parser.nextEvent();
                license = parser.getString();
              } else if(key.equals("timestamp")) {
                parser.nextEvent();
              } else if(key.equals("base")) {
                parser.nextEvent();
                baseCurrency = parser.getString();
              } else if(key.equals("rates")) {
                ev = parser.nextEvent();
                assert(ev == JSONParser.OBJECT_START);
                ev = parser.nextEvent();
                while (ev != JSONParser.OBJECT_END) {
                  String curr = parser.getString();
                  ev = parser.nextEvent();
                  Double rate = parser.getDouble();
                  rates.put(curr, rate);
                  ev = parser.nextEvent();                  
                }
              } else {
                log.warn("Unknown key {}", key);
              }
              break;
            } else {
              log.warn("Expected key, got {}", JSONParser.getEventString(ev));
              break;
            }

          case JSONParser.OBJECT_END:
          case JSONParser.OBJECT_START:
          case JSONParser.EOF:
            break;

          default:
            if (log.isInfoEnabled()) {
              log.info("Noggit UNKNOWN_EVENT_ID: {}", JSONParser.getEventString(ev));
            }
            break;
        }
      } while( ev != JSONParser.EOF);

      // We set the timestamp based on the system time not the time from openexchangerates.com because
      // in the method reloadIfExpired we will be comparing it to the system time. If we took the time
      // from openexchangerates.com and there was a desynchronization (or some other error), we could
      // be refreshing the exchange rates too often or too rarely.
      timestamp = System.currentTimeMillis() / 1000;
    }

    public Map<String, Double> getRates() {
      return rates;
    }
    
    public long getTimestamp() {
      return timestamp;
    }
    /** Package protected method for test purposes
     * @lucene.internal
     */
    void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public String getDisclaimer() {
      return disclaimer;
    }

    public String getBaseCurrency() {
      return baseCurrency;
    }

    public String getLicense() {
      return license;
    }
  }
}
