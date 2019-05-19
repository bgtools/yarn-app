package io.github.shenbinglife.hadoop.yarnapp.am;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  private final YarnConfiguration conf;
  private AmOpts amOpts;

  private AMRMClientAsync amRMClient;
  private final NMClientAsync nmClientAsync;

  public ApplicationMaster(String[] args) {
    LOG.info("ApplicationMaster args:" + String.join(" ", args));

    this.amOpts = AmOpts.build(args);
    this.conf = new YarnConfiguration();

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    NMCallbackHandler containerListener = new NMCallbackHandler();
    this.nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();
  }

  public static void main(String[] args) throws IOException, YarnException, InterruptedException {
    ApplicationMaster appMaster = new ApplicationMaster(args);
    appMaster.run();
  }

  private void run() throws IOException, YarnException, InterruptedException {
    // Register self with ResourceManager
    // This will start heartbeating to the RM
    String appMasterHostname = NetUtils.getHostname();
    amRMClient.registerApplicationMaster(appMasterHostname, -1, "");

    System.out.println(amOpts);

    amRMClient.waitFor(() -> false, 5000);
  }
}
