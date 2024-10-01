package org.apache.solr.storage;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.junit.Before;
import org.junit.Test;

public class SizeAwareDirectoryTest extends SolrTestCaseJ4 {

  private final ConcurrentHashMap<String, Boolean> activeFiles = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> deletedFiles = new ConcurrentHashMap<>();
  private String path;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    activeFiles.clear();
    deletedFiles.clear();
    path = createTempDir().toString() + "/somedir";
  }

  @Test
  public void testSizeTracking() throws Exception {
    // after onDiskSize has been init(), the size should be correct using the LongAdder sum in
    // SizeAwareDirectory
    CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();
    try (dirFac) {
      dirFac.initCoreContainer(null);
      dirFac.init(new NamedList<>());

      Directory dir =
          dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
      try {
        try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
          file.writeInt(42);
        } // implicitly close file

        long expectedDiskSize = Files.size(Paths.get(path + "/test_file"));
        assertEquals(
            "directory size should be equal to on disk size of test files",
            expectedDiskSize,
            dirFac.onDiskSize(dir));

        try (IndexOutput file = dir.createOutput("test_file2", IOContext.DEFAULT)) {
          file.writeInt(84);
        } // implicitly close file

        expectedDiskSize =
            Files.size(Paths.get(path + "/test_file"))
                + Files.size(Paths.get(path + "/test_file2"));
        assertEquals(
            "directory size should be equal to on disk size of test files",
            expectedDiskSize,
            dirFac.onDiskSize(dir));
      } finally {
        dirFac.release(dir);
      }
    }
  }

  @Test
  public void testDelete() throws Exception {
    // write a file, then another, then delete one of the files - the onDiskSize should update
    // correctly
    CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();
    try (dirFac) {
      dirFac.initCoreContainer(null);
      dirFac.init(new NamedList<>());

      Directory dir =
          dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
      try {
        // small file first to initSize()
        try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
          file.writeInt(42);
        } // implicitly close file

        long expectedDiskSize = Files.size(Paths.get(path + "/test_file"));
        assertEquals(
            "directory size should be equal to on disk size of test files",
            expectedDiskSize,
            dirFac.onDiskSize(dir));

        writeBlockSizeFile(dir, "test_file2");

        expectedDiskSize =
            Files.size(Paths.get(path + "/test_file"))
                + Files.size(Paths.get(path + "/test_file2"));
        assertEquals(
            "directory size should be equal to on disk size of test files",
            expectedDiskSize,
            dirFac.onDiskSize(dir));

        deleteFile(dir, "test_file2");
        expectedDiskSize = Files.size(Paths.get(path + "/test_file"));
        assertEquals(
            "directory size should be equal to on disk size of test files",
            expectedDiskSize,
            dirFac.onDiskSize(dir));
      } finally {
        dirFac.release(dir);
      }
    }
  }

  @Test
  public void testSimultaneous() throws Exception {
    CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();
    try (dirFac) {
      dirFac.initCoreContainer(null);
      dirFac.init(new NamedList<>());

      Directory dir =
          dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
      try {
        String createPrefix = "test_first_";
        Random r = new Random(42);
        int numCreateThreads = 100;
        List<Thread> createThreads = getCreateThreads(dir, numCreateThreads, r, createPrefix);
        startThreads(createThreads);
        waitForThreads(createThreads);

        List<Thread> createAndDeleteThreads =
            getCreateThreads(dir, numCreateThreads, r, "test_second_");
        // randomly delete 10 percent of the files created above, while also creating some files
        createAndDeleteThreads.addAll(
            getRandomDeleteThreads(dir, activeFiles.size() / 10, createPrefix, r));
        startThreads(createAndDeleteThreads);
        waitForThreads(createAndDeleteThreads);

        long expected = expectedDiskSizeForFiles();
        long actual = dirFac.onDiskSize(dir);
        assertEquals(
            "directory size should be equal to on disk size of test files", expected, actual);
      } finally {
        dirFac.release(dir);
      }
    }
  }

  private void waitForThreads(List<Thread> threads) {
    for (int i = 0; i < threads.size(); i++) {
      try {
        threads.get(i).join();
      } catch (InterruptedException e) {
        System.out.println("Thread was interrupted");
        fail("thread was interrupted " + i);
      }
    }
  }

  private List<Thread> getCreateThreads(Directory dir, int numThreads, Random r, String prefix) {
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      String name = prefix + i;
      int size = r.nextInt(100000000) + CompressingDirectory.COMPRESSION_BLOCK_SIZE + 1;
      threads.add(new Thread(new CreateFileTask(dir, name, size)));
    }
    return threads;
  }

  private List<Thread> getRandomDeleteThreads(
      Directory dir, int numThreads, String prefix, Random r) {
    List<String> activeFilesFiltered =
        activeFiles.keySet().stream()
            .filter(s -> s.startsWith(prefix))
            .collect(Collectors.toList());
    // can't delete more files than exist
    assertTrue(activeFilesFiltered.size() >= numThreads);
    List<String> filesToDelete = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      int index = r.nextInt(activeFilesFiltered.size());
      filesToDelete.add(activeFilesFiltered.remove(index));
    }

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      threads.add(new Thread(new DeleteFileTask(dir, filesToDelete.get(i))));
    }
    return threads;
  }

  private void startThreads(List<Thread> threads) {
    for (Thread t : threads) {
      t.start();
    }
  }

  private long expectedDiskSizeForFiles() throws Exception {
    long fileSize = 0;
    for (String name : activeFiles.keySet()) {
      fileSize += Files.size(Paths.get(path + "/" + name));
    }
    return fileSize;
  }

  private class CreateFileTask implements Runnable {
    final String name;
    final int size;
    final Directory dir;

    public CreateFileTask(Directory dir, String name, int size) {
      this.name = name;
      this.size = size;
      this.dir = dir;
    }

    @Override
    public void run() {
      try {
        writeRandomFileOfSize(dir, name, size);
      } catch (Exception e) {
        fail("exception writing file" + name);
      }
    }
  }

  private class DeleteFileTask implements Runnable {
    final String name;
    final Directory dir;

    public DeleteFileTask(Directory dir, String name) {
      this.dir = dir;
      this.name = name;
    }

    @Override
    public void run() {
      try {
        deleteFile(dir, name);
      } catch (Exception e) {
        fail("exception deleting file" + name);
      }
    }
  }

  private void deleteFile(Directory dir, String name) throws Exception {
    dir.deleteFile(name);
    activeFiles.remove(name);
    addToDeletedFiles(name);
  }

  private void writeBlockSizeFile(Directory dir, String name) throws Exception {
    try (IndexOutput file = dir.createOutput(name, IOContext.DEFAULT)) {
      // write some small things first to force past blocksize boundary
      file.writeInt(42);
      file.writeInt(84);
      // write a giant blocksize thing to force compression with dump()
      Random random = new Random(42);
      int blocksize = CompressingDirectory.COMPRESSION_BLOCK_SIZE;
      byte[] byteArray = new byte[blocksize];
      random.nextBytes(byteArray);
      file.writeBytes(byteArray, blocksize);
    } // implicitly close file
    addToActiveFiles(name);
  }

  private void writeRandomFileOfSize(Directory dir, String name, int size) throws Exception {
    try (IndexOutput file = dir.createOutput(name, IOContext.DEFAULT)) {
      // write a giant blocksize thing to force compression with dump()
      Random random = new Random(42);
      int bufferSize = 4096; // Chunk size
      byte[] buffer = new byte[bufferSize];
      int remainingBytes = size;

      while (remainingBytes > 0) {
        int bytesToWrite = Math.min(remainingBytes, bufferSize);
        random.nextBytes(buffer);

        file.writeBytes(buffer, bytesToWrite);

        remainingBytes -= bytesToWrite;
      }
    } // implicitly close file
    addToActiveFiles(name);
  }

  private void addToActiveFiles(String name) {
    activeFiles.put(name, true);
  }

  private void addToDeletedFiles(String name) {
    deletedFiles.put(name, true);
  }
}
