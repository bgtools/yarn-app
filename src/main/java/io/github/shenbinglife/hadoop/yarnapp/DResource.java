package io.github.shenbinglife.hadoop.yarnapp;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

public class DResource {

  private final String name;
  private final LocalResourceType type;
  private final String resourcePath;

  DResource(String name, LocalResourceType type, String resourcePath) {
    this.name = name;
    this.type = type;
    this.resourcePath = resourcePath;
  }

  public Path getPath(Map<String, String> env) {
    return new Path(env.get(getLocationEnvVar()));
  }

  public long getTimestamp(Map<String, String> env) {
    return Long.parseLong(env.get(getTimestampEnvVar()));
  }

  public long getLength(Map<String, String> env) {
    return Long.parseLong(env.get(getLengthEnvVar()));
  }

  public String getLocationEnvVar() {
    return name + "_LOCATION";
  }

  public String getTimestampEnvVar() {
    return name + "_TIMESTAMP";
  }

  public String getLengthEnvVar() {
    return name + "_LENGTH";
  }

  public LocalResourceType getType() {
    return type;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public String toString() {
    return name;
  }

}
