package io.github.shenbinglife.hadoop.yarnapp;

import java.util.Arrays;
import org.apache.commons.lang.ArrayUtils;

public class Utils {
  private static final String[] ARCHIVE_FILE_TYPES = { ".zip", ".tar", ".tgz", ".tar.gz" };

  public static boolean isArchive(String fileName) {
    return Arrays.stream(ARCHIVE_FILE_TYPES).noneMatch(suffix -> fileName.endsWith(suffix));
  }

}
