package io.github.shenbinglife.hadoop.yarnapp;

import org.apache.hadoop.yarn.api.records.LocalResourceType;

public class DConstants {

  public static final String REMOTE_STORAGE_DIR = ".yarnapp";
  public static final String REMOTE_STORAGE_PATH_ENV = "REMOTE_STORAGE_PATH";

  public static final DResource JAR_DEPENDENCIES = new DResource("JAR_DEPENDENCIES", LocalResourceType.ARCHIVE, "dependencies");
  public static final DResource LOG4J = new DResource("LOG4J", LocalResourceType.FILE, "log4j");
}
