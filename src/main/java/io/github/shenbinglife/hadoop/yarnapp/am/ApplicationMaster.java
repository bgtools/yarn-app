package io.github.shenbinglife.hadoop.yarnapp.am;

import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocol;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocolService;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
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
  private AmProtocolService amProtocolService;


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

    amProtocolService = new AmProtocolService();
    amProtocolService.init(conf);
    amProtocolService.start();
  }

  public static void main(String[] args) throws IOException, YarnException, InterruptedException {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster(args);
      appMaster.run();
      result = appMaster.stop();
    } catch (Exception e) {
      LOG.fatal("Error running ApplicationMaster", e);
      ExitUtil.terminate(1, e);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  private boolean run() throws IOException, YarnException, InterruptedException {
    // Register self with ResourceManager
    // This will start heartbeating to the RM
    String appMasterHostname = NetUtils.getHostname();
    amRMClient.registerApplicationMaster(appMasterHostname, -1, "");

    System.out.println(amOpts);

    amRMClient.waitFor(() -> false, 5000);
    return stop();
  }

  private boolean stop() {
    this.nmClientAsync.stop();
    this.amProtocolService.stop();

    FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
    String appMessage = "Succeed";

    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    amRMClient.stop();
    return true;
  }
}
