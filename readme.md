# yarn-app

基于HDFS Yarn开发的一个ApplicationMaster应用，基于该项目模版可以开发定制性的Yarn APP。  
参考：[Hadoop: Writing YARN Applications](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html)

目前该项目已经实现的功能：
1. 通过客户端提交自定义的ApplicationMaster并执行
2. 自定义了AmProtocolService的RPC服务，允许ApplicationMaster与客户端通信

## 使用方法

###Package
```bash
gradle build
```

### Exec
执行下面的启动命令即可，按`Ctrl + C`可以终止该Yarn应用。  
更多的启动参数见`io.github.shenbinglife.hadoop.yarnapp.ClientOpts`。
```bash
hadoop jar yarn-app-1.0-SNAPSHOT.jar
```

示例结果：
```bash
19/05/25 19:46:23 INFO yarnapp.Client: Got Cluster metric info from ASM, numNodeManagers=3
19/05/25 19:46:23 INFO yarnapp.Client: Queue info, queueName=default, queueCurrentCapacity=0.0, queueMaxCapacity=1.0, queueApplicationCount=0, queueChildQueueCount=0
19/05/25 19:46:23 INFO yarnapp.Client: Max mem capabililty of resources in this cluster 8192
19/05/25 19:46:23 INFO yarnapp.Client: Max virtual cores capabililty of resources in this cluster 8
19/05/25 19:46:23 INFO yarnapp.Client: Setting up app master command
19/05/25 19:46:23 INFO yarnapp.Client: Completed setting up app master command: {{JAVA_HOME}}/bin/java -Xmx2048m io.github.shenbinglife.hadoop.yarnapp.am.ApplicationMaster --container_mem 1024 --container_vcores 1 --container_priority 1 --num_container 1 1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr
19/05/25 19:46:23 INFO yarnapp.Client: Set the environment for the application master
19/05/25 19:46:23 INFO yarnapp.Client: Uploading resource JAR_DEPENDENCIES from [/home/omm/yarn-app-1.0-SNAPSHOT.jar] to hdfs://ns/user/omm/.yarnapp/application_1558783382130_0006/dependencies.zip
19/05/25 19:46:24 INFO impl.YarnClientImpl: Submitted application application_1558783382130_0006
19/05/25 19:46:25 INFO yarnapp.Client: Track the application at: http://host-192-168-199-100:23188/proxy/application_1558783382130_0006/
19/05/25 19:46:25 INFO yarnapp.Client: Kill the application using: yarn application -kill application_1558783382130_0006
19/05/25 19:46:39 INFO yarnapp.Client: amClient getAMState:AM started
19/05/25 19:46:41 INFO yarnapp.Client: amClient getAMState:AM started
19/05/25 19:46:42 INFO yarnapp.Client: amClient getAMState:AM started
....

19/05/25 19:47:38 INFO yarnapp.Client: killing application application_1558783382130_0006
19/05/25 19:47:38 INFO impl.YarnClientImpl: Killed application application_1558783382130_0006
19/05/25 19:47:38 INFO yarnapp.Client: Client stopped.

```