package io.github.shenbinglife.hadoop.yarnapp.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.yarn.proto.AmProtocol.AmProtocolService;

/**
 * ProtocolInfo注解必须添加，指定该RPC接口的版本号
 */
@ProtocolInfo(
    protocolName = "io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocolPB",
    protocolVersion = 1)
public interface AmProtocolPB extends AmProtocolService.BlockingInterface {

}
