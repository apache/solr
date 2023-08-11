package org.apache.solr.util;

import java.io.File;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class RegexFileFilterTest extends SolrTestCase {

  private final RegexFileFilter endsWithDotTxt = new RegexFileFilter(".*\\.txt$");
  private final RegexFileFilter alphaWithTxtOrPdfExt = new RegexFileFilter("^[a-z]+\\.(txt|pdf)$");

  @Test
  public void testAcceptTrue() {
    assertTrue(endsWithDotTxt.accept(new File("/foo/bar/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("~/baz.txt")));
    assertTrue(endsWithDotTxt.accept(new File("~/1234-abc_.txt")));
    assertTrue(endsWithDotTxt.accept(new File(".txt")));
    assertTrue(alphaWithTxtOrPdfExt.accept(new File("/foo/bar.txt")));
    assertTrue(alphaWithTxtOrPdfExt.accept(new File("/foo/baz.pdf")));
  }

  @Test
  public void testAcceptFalse() {
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.tx")));
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.txts")));
    assertFalse(endsWithDotTxt.accept(new File("/foo/bar.exe")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("/foo/bar/b4z.txt")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("/foo/bar/baz.jpg")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("~/foo-bar.txt")));
    assertFalse(alphaWithTxtOrPdfExt.accept(new File("~/foobar123.txt")));
  }
}
