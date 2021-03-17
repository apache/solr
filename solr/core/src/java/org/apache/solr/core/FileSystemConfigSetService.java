package org.apache.solr.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Solr standalone version of File System ConfigSetService.
 * <p>
 * Loads a ConfigSet defined by the core's configSet property,
 * looking for a directory named for the configSet property value underneath
 * a base directory.  If no configSet property is set, loads the ConfigSet
 * instead from the core's instance directory.
 */
public class FileSystemConfigSetService extends ConfigSetService {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Path configSetBase;

    public FileSystemConfigSetService(SolrResourceLoader loader, boolean shareSchema, Path configSetBase) {
        super(loader, shareSchema);
        this.configSetBase = configSetBase;
    }

    @Override
    public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
        Path instanceDir = locateInstanceDir(cd);
        SolrResourceLoader solrResourceLoader = new SolrResourceLoader(instanceDir, parentLoader.getClassLoader());
        return solrResourceLoader;
    }

    @Override
    public String configSetName(CoreDescriptor cd) {
        return (cd.getConfigSet() == null ? "instancedir " : "configset ") + locateInstanceDir(cd);
    }

    protected Path locateInstanceDir(CoreDescriptor cd) {
        String configSet = cd.getConfigSet();
        if (configSet == null)
            return cd.getInstanceDir();
        Path configSetDirectory = configSetBase.resolve(configSet);
        if (!Files.isDirectory(configSetDirectory))
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "Could not load configuration from directory " + configSetDirectory);
        return configSetDirectory;
    }

    @Override
    protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFileName) {
        Path schemaFile = Paths.get(solrConfig.getResourceLoader().getConfigDir()).resolve(schemaFileName);
        try {
            return Files.getLastModifiedTime(schemaFile).toMillis();
        } catch (FileNotFoundException e) {
            return null; // acceptable
        } catch (IOException e) {
            log.warn("Unexpected exception when getting modification time of {}", schemaFile, e);
            return null; // debatable; we'll see an error soon if there's a real problem
        }
    }

}
