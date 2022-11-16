package org.apache.solr.core;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.ApiSupport;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrRequestHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.apache.solr.api.ApiBag.HANDLER_NAME;

public class RequestHandlerBag extends PluginBag<SolrRequestHandler> {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private  ApiBag apiBag;
    private  ResourceConfig jerseyResources;
    private  JerseyMetricsLookupRegistry infoBeanByResource;

    public RequestHandlerBag(SolrCore core) {
        super(SolrRequestHandler.class, core, true);
        this.apiBag = new ApiBag(core != null);
    }

    @Override
    public PluginHolder<SolrRequestHandler> put(String name, PluginHolder<SolrRequestHandler> plugin) {
        Boolean registerApi = null;
        Boolean disableHandler = null;
        if (plugin.pluginInfo != null) {
            String registerAt = plugin.pluginInfo.attributes.get("registerPath");
            if (registerAt != null) {
                List<String> strs = StrUtils.splitSmart(registerAt, ',');
                disableHandler = !strs.contains("/solr");
                registerApi = strs.contains("/v2");
            }
        }

        if (apiBag != null) {
            if (plugin.isLoaded()) {
                SolrRequestHandler inst = plugin.get();
                if (inst instanceof ApiSupport) {
                    ApiSupport apiSupport = (ApiSupport) inst;
                    if (registerApi == null) registerApi = apiSupport.registerV2();
                    if (disableHandler == null) disableHandler = !apiSupport.registerV1();

                    if (registerApi) {
                        Collection<Api> apis = apiSupport.getApis();
                        if (apis != null) {
                            Map<String, String> nameSubstitutes = singletonMap(HANDLER_NAME, name);
                            for (Api api : apis) {
                                apiBag.register(api, nameSubstitutes);
                            }
                        }

                        // TODO Should we use <requestHandler name="/blah"> to override the path that each
                        //  resource registers under?
                        Collection<Class<? extends JerseyResource>> jerseyApis =
                                apiSupport.getJerseyResources();
                        if (!CollectionUtils.isEmpty(jerseyApis)) {
                            for (Class<? extends JerseyResource> jerseyClazz : jerseyApis) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Registering jersey resource class: {}", jerseyClazz.getName());
                                }
                                jerseyResources.register(jerseyClazz);
                                // See MetricsBeanFactory javadocs for a better understanding of this resource->RH
                                // mapping
                                if (inst instanceof RequestHandlerBase) {
                                    infoBeanByResource.put(jerseyClazz, (RequestHandlerBase) inst);
                                }
                            }
                        }
                    }
                }
            } else {
                if (registerApi != null && registerApi)
                    apiBag.registerLazy((PluginHolder<SolrRequestHandler>) plugin, plugin.pluginInfo);
            }
        }
        PluginHolder<SolrRequestHandler> old = null;
        if (disableHandler == null) disableHandler = Boolean.FALSE;
        if (!disableHandler) old = registry.put(name, plugin);
        if (plugin.pluginInfo != null && plugin.pluginInfo.isDefault()) setDefault(name);
        if (plugin.isLoaded()) registerMBean(plugin.get(), core, name);
        // old instance has been replaced - close it to prevent mem leaks
        if (old != null && old != plugin) {
            closeQuietly(old);
        }
        return old;
    }

    public Api v2lookup(String path, String method, Map<String, String> parts) {
        if (apiBag == null) {
            throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "this should not happen, looking up for v2 API at the wrong place");
        }
        return apiBag.lookup(path, method, parts);
    }



    public ApiBag getApiBag() {
        return apiBag;
    }

    public ResourceConfig getJerseyEndpoints() {
        return jerseyResources;
    }

    public static class JerseyMetricsLookupRegistry
        extends HashMap<Class<? extends JerseyResource>, RequestHandlerBase> {}
}
