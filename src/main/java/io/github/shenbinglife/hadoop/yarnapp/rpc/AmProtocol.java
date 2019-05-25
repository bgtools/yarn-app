package io.github.shenbinglife.hadoop.yarnapp.rpc;

import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponse;
import java.io.IOException;

public interface AmProtocol {
  AmStateResponse getAMState() throws IOException;
}
