package io.github.shenbinglife.hadoop.yarnapp.rpc.impl.pb.client;

import com.google.protobuf.ServiceException;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocol;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocolPB;
import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos;
import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos.AmStateRequestProto;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateRequest;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponse;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponsePBImpl;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;

public class AmProtocolPBClientImpl implements AmProtocol, Closeable {

  private AmProtocolPB proxy;

  public AmProtocolPBClientImpl() {
  }

  public AmProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {
    RPC.setProtocolEngine(conf, AmProtocolPB.class, ProtobufRpcEngine.class);
    proxy = RPC.getProxy(AmProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public AmStateResponse getAMState() throws IOException {
    AmStateRequestProto request = AmStateRequestProto.getDefaultInstance();
    try {
      AmProtocolProtos.AmStateResponseProto response = proxy.getAMState(null, request);
      return new AmStateResponsePBImpl(response);
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  private IOException unwrapAndThrowException(ServiceException se) {
    if (se.getCause() instanceof RemoteException) {
      return ((RemoteException) se.getCause()).unwrapRemoteException();
    } else if (se.getCause() instanceof IOException) {
      return (IOException) se.getCause();
    } else {
      throw new UndeclaredThrowableException(se.getCause());
    }
  }
}
