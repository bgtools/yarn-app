package io.github.shenbinglife.hadoop.yarnapp.am;

import java.util.List;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

public class RMCallbackHandler implements  AMRMClientAsync.CallbackHandler {

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {

  }

  @Override
  public void onContainersAllocated(List<Container> containers) {

  }

  @Override
  public void onShutdownRequest() {

  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {

  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void onError(Throwable e) {

  }
}
