package org.apache.solr.common;

import org.apache.solr.common.cloud.SolrClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ModulesClassLoader implements SolrClassLoader, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Class<?>[] NO_CLASSES = new Class<?>[0];
    private static final Object[] NO_OBJECTS = new Object[0];

    private URLClassLoader classLoader;

    public ModulesClassLoader() {
        this(null);
    }

    public ModulesClassLoader(ClassLoader parent) {
        if (parent == null) {
            parent = getClass().getClassLoader();
        }
        this.classLoader = URLClassLoader.newInstance(new URL[0], parent);
    }

    @Override
    public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
        return newInstance(cname, expectedType, subpackages, NO_CLASSES, NO_OBJECTS);
    }

    @Override
    public <T> T newInstance(String cName, Class<T> expectedType, String[] subPackages,
                             Class<?>[] params, Object[] args) {
        Class<? extends T> clazz = findClass(cName, expectedType);
        if (clazz == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "Can not find class: " + cName + " in " + classLoader);
        }

        T obj;
        try {
            Constructor<? extends T> constructor;
            try {
                constructor = clazz.getConstructor();
                obj = constructor.newInstance();
                return obj;
            } catch (Exception e1) {
                e1.printStackTrace();
                throw e1;
            }
        } catch (Error err) {
            log.error("Loading Class {} ({}) triggered serious java error: {}",
                    cName, clazz.getName(), err.getClass().getName(), err);
            throw err;
        } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "Error instantiating class: '" + clazz.getName() + "'", e);
        }
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
        try {
            return Class.forName(cname, true, classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    cname + " Error loading class '" + cname + "'", e);
        }
    }


    public synchronized void addToClassLoader(List<URL> urls) {
        URLClassLoader newLoader = addURLsToClassLoader(classLoader, urls);
        if (newLoader == classLoader) {
            return; // short-circuit
        }

        this.classLoader = newLoader;

        if (log.isInfoEnabled()) {
            log.info(
                    "Added {} libs to classloader, from paths: {}",
                    urls.size(),
                    urls.stream()
                            .map(u -> u.getPath().substring(0, u.getPath().lastIndexOf("/")))
                            .sorted()
                            .distinct()
                            .collect(Collectors.toList()));
        }
    }

    private static URLClassLoader addURLsToClassLoader(
            final URLClassLoader oldLoader, List<URL> urls) {
        if (urls.size() == 0) {
            return oldLoader;
        }

        List<URL> allURLs = new ArrayList<>();
        allURLs.addAll(Arrays.asList(oldLoader.getURLs()));
        allURLs.addAll(urls);
        for (URL url : urls) {
            if (log.isDebugEnabled()) {
                log.debug("Adding '{}' to classloader", url);
            }
        }

        ClassLoader oldParent = oldLoader.getParent();
        return URLClassLoader.newInstance(allURLs.toArray(new URL[allURLs.size()]), oldParent);
    }

    @Override
    public void close() throws IOException {
        try {
            if (classLoader != null) {
                classLoader.close();
            }
        } catch (IOException t) {
            log.warn("Unable to close URLClassLoader in ModulesClassLoader");
        }
    }
}
