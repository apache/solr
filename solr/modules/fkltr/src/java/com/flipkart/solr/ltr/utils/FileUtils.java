package com.flipkart.solr.ltr.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

public class FileUtils {

  public static String readStringFromFile(String fileName) {
    try {
      return IOUtils.toString(new FileInputStream(fileName), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new RuntimeException("unable to read file");
    }
  }

}
