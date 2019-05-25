package io.github.shenbinglife.hadoop.yarnapp.rpc.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocol;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocolPB;
import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos.AmStateRequestProto;
import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos.AmStateResponseProto;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponse;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponsePBImpl;
import java.io.IOException;

public class AmProtocolPBServiceImpl implements AmProtocolPB {

  private AmProtocol real;

  public AmProtocolPBServiceImpl(AmProtocol real) {
    this.real = real;
  }

  @Override
  public AmStateResponseProto getAMState(RpcController controller, AmStateRequestProto request)
      throws ServiceException {
    try {
      AmStateResponse name = real.getAMState();
      return ((AmStateResponsePBImpl)name).getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
