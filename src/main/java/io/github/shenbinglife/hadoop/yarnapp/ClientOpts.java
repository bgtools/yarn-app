package io.github.shenbinglife.hadoop.yarnapp;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.github.shenbinglife.hadoop.yarnapp.am.AmOpts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientOpts extends AmOpts {

  @Parameter(names = "--app_name")
  public String appName = "MyYarnApp";

  @Parameter(names = "--queue")
  public String amQueue = "default";

  @Parameter(names = "--am_mem")
  public int amMemory = 2048;

  @Parameter(names = "--am_vcores")
  public int amVCores = 1;

  @Parameter(names = "--debug")
  public boolean debugFlag = false;

  @Parameter(names = "--am_priority")
  public int amPriority = 1;

  @Parameter(names = "--remote_storage")
  public String remoteStorage;

  @Parameter(names = "--client_timeout")
  public long clientTimeout = -1;

  @Parameter(names = "--shell_env")
  public List<String> shellEnvs = new ArrayList<>();


  public static ClientOpts build(String[] args) {
    ClientOpts opts = new ClientOpts();
    JCommander commander = new JCommander(opts);
    commander.parse(args);
    return opts;
  }

  public Map<String, String> getShellEnv() {
    Map<String, String> env = new HashMap<>();
    for (String it : shellEnvs) {
      int index = it.indexOf("=");
      if (index < 0) {
        continue;
      }
      String key = it.substring(0, index).trim();
      String value = it.substring(index + 1).trim();
      env.put(key, value);
    }
    return env;
  }
}
