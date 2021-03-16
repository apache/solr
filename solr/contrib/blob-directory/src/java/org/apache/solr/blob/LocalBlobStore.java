package org.apache.solr.blob;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.params.SolrParams;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class LocalBlobStore extends BlobStore {

    private String blobRootDir;

    public String getBlobRootDir() {
        return blobRootDir;
    }

    @Override
    public void init(SolrParams params) {
        blobRootDir = params.get("blobStore.rootDir");
        if (blobRootDir == null) {
            throw new IllegalArgumentException("blobStore.rootDir is required");
        }
    }

    @Override
    public void create(String dirPath, String fileName, InputStream inputStream, long contentLength) throws IOException {
        File file = new File(new File(blobRootDir, dirPath), fileName);
        if (file.exists()) {
            throw new IOException("\"" + file + "\" already exists");
        }
        FileUtils.copyToFile(inputStream, file);
    }

    @Override
    public void delete(String dirPath, Collection<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            File file = new File(new File(blobRootDir, dirPath), fileName);
            if (!file.delete() && file.exists()) {
                throw new IOException("IO error while deleting file \"" + file + "\"");
            }
        }
    }

    @Override
    public void deleteDirectory(String dirPath) throws IOException {
        File dir = new File(blobRootDir, dirPath);
        if (!dir.isDirectory()) {
            if (dir.exists()) {
                throw new IOException("\"" + dir + "\" is not a directory");
            }
            return;
        }
        FileUtils.deleteDirectory(dir);
        if (dir.exists()) {
            throw new IOException("IO error while deleting file \"" + dir + "\"");
        }
    }

    @Override
    public List<String> listInDirectory(String dirPath, Predicate<String> nameFilter) throws IOException {
        File dir = new File(blobRootDir, dirPath);
        if (!dir.exists()) {
            return Collections.emptyList();
        }
        String[] list = dir.list((__, name) -> nameFilter.test(name));
        if (list == null) {
            if (!dir.isDirectory()) {
                throw new IOException("\"" + dir + "\" is not a directory");
            }
            throw new IOException("IO error while listing in directory \"" + dir + "\"");
        }
        if (list.length == 0) {
            return Collections.emptyList();
        }
        Arrays.sort(list);
        return Arrays.asList(list);
    }

    @Override
    public InputStream read(String dirPath, String fileName) throws IOException {
        return new FileInputStream(new File(new File(blobRootDir, dirPath), fileName));
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
