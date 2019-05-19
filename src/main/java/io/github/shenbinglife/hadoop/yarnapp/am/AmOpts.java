package io.github.shenbinglife.hadoop.yarnapp.am;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class AmOpts {

  @Parameter(names = {"--container_mem"})
  public int containerMemory = 1024;

  @Parameter(names = "--container_vcores")
  public int containerVirtualCores = 1;

  @Parameter(names = "--num_container")
  public int numContainers = 1;

  @Parameter(names = "--container_priority")
  public int containerPriority = 1;

  public static AmOpts build(String[] args) {
    AmOpts opts = new AmOpts();
    JCommander commander = new JCommander(opts);
    commander.parse(args);
    return opts;
  }

}
