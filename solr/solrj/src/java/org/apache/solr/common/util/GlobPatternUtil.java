package org.apache.solr.common.util;

import java.nio.file.FileSystems;
import java.nio.file.Paths;

public class GlobPatternUtil {

    public static boolean matches(String pattern, String input) {
        return FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Paths.get(input));
    }
}
