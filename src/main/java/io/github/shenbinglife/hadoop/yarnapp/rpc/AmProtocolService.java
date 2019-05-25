package io.github.shenbinglife.hadoop.yarnapp.rpc;

import io.github.shenbinglife.hadoop.yarnapp.DConstants;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponse;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponsePBImpl;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class AmProtocolService extends AbstractService implements AmProtocol {

  private static final Log LOG = LogFactory.getLog(AmProtocolService.class);

  private Server server;
  InetSocketAddress clientBindAddress;

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public AmProtocolService(String name) {
    super(AmProtocolService.class.getName());
  }

  public AmProtocolService() {
    super(AmProtocolService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.clientBindAddress = conf.getSocketAddr(DConstants.AM_PROTOCOL_SERVER_ADDRESS,
        DConstants.DEFAULT_AM_PROTOCOL_SERVER_ADDRESS,
        DConstants.DEFAULT_AM_PROTOCOL_PORT);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    this.server =
        rpc.getServer(AmProtocol.class, this,
            clientBindAddress,
            conf, null, // Secret manager null for now (security not supported)
            50);
    this.server.start();

    clientBindAddress = server.getListenerAddress();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  public AmStateResponse getAMState() throws IOException {
    AmStateResponsePBImpl amStateResponsePB = new AmStateResponsePBImpl();
    amStateResponsePB.setAMState("AM started");
    return amStateResponsePB;
  }
}
