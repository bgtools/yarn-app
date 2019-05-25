package io.github.shenbinglife.hadoop.yarnapp;

import org.apache.hadoop.yarn.api.records.LocalResourceType;

public class DConstants {

  public static final String REMOTE_STORAGE_DIR = ".yarnapp";
  public static final String REMOTE_STORAGE_PATH_ENV = "REMOTE_STORAGE_PATH";

  public static final DResource JAR_DEPENDENCIES = new DResource("JAR_DEPENDENCIES", LocalResourceType.ARCHIVE, "dependencies");
  public static final DResource LOG4J = new DResource("LOG4J", LocalResourceType.FILE, "log4j");

  //----------------Configurations----------------------------
  public static final int DEFAULT_AM_PROTOCOL_PORT = 50066;
  public static final String DEFAULT_AM_PROTOCOL_SERVER_ADDRESS = "0.0.0.0:50066";
  public static final String AM_PROTOCOL_SERVER_ADDRESS = "yarnapp.amprotocol.address";
}
