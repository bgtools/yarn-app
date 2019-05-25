package io.github.shenbinglife.hadoop.yarnapp.rpc.records;

import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos;
import io.github.shenbinglife.hadoop.yarnapp.rpc.proto.records.AmProtocolProtos.AmStateResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

public class AmStateResponsePBImpl extends ProtoBase<AmProtocolProtos.AmStateResponseProto> implements
    AmStateResponse {

  AmProtocolProtos.AmStateResponseProto proto = AmProtocolProtos.AmStateResponseProto.getDefaultInstance();
  AmProtocolProtos.AmStateResponseProto.Builder builder = null;
  boolean viaProto = false;

  public AmStateResponsePBImpl() {
    builder = AmProtocolProtos.AmStateResponseProto.newBuilder();
  }

  public AmStateResponsePBImpl(
      AmProtocolProtos.AmStateResponseProto proto) {
    this.proto = proto;
    viaProto = true;

  }

  @Override
  public String getAMState() {
    return getProto().getState();
  }

  @Override
  public void setAMState(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearState();
      return;
    }
    builder.setState(name);
  }

  @Override
  public AmProtocolProtos.AmStateResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AmStateResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
