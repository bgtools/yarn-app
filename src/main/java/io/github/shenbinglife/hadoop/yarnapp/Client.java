package io.github.shenbinglife.hadoop.yarnapp;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.github.shenbinglife.hadoop.yarnapp.am.ApplicationMaster;
import io.github.shenbinglife.hadoop.yarnapp.rpc.AmProtocol;
import io.github.shenbinglife.hadoop.yarnapp.rpc.records.AmStateResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Client extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(Client.class);

  private String[] jarFile;
  private FileSystem fs;
  private ClientOpts clientOpts;
  private YarnClient yarnClient;
  private ApplicationId appId;
  private Class<?> appMasterMainClass = ApplicationMaster.class;
  private long clientStartTime;


  public Client(String... dependencies) {
    this.jarFile = dependencies;

  }

  public static void main(String[] args) throws Exception {
    String jar = ClassUtil.findContainingJar(Client.class);
    Client client = new Client(jar);
    System.exit(ToolRunner.run(new YarnConfiguration(), client, args));
  }

  @Override
  public int run(String[] args) throws Exception {
    this.clientStartTime = System.currentTimeMillis();
    this.clientOpts = ClientOpts.build(args);
    this.fs = FileSystem.get(getConf());

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(getConf());
    yarnClient.start();

    setupApplication();
    setupShutdownHook();
    return waitForCompletion();
  }

  private int waitForCompletion() throws IOException, YarnException {
    int exitCode = 0;
    boolean loggedApplicationInfo = false;
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      if (report.getTrackingUrl() != null && !loggedApplicationInfo) {
        loggedApplicationInfo = true;
        LOG.info("Track the application at: " + report.getTrackingUrl());
        LOG.info("Kill the application using: yarn application -kill " + report.getApplicationId());
      }

      YarnApplicationState applicationState = report.getYarnApplicationState();
      LOG.debug("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + applicationState.toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      if (applicationState == YarnApplicationState.KILLED) {
        LOG.info("app was killed; exiting from client.");
        break;
      } else if (applicationState == YarnApplicationState.FINISHED
          || applicationState == YarnApplicationState.FAILED) {
        LOG.info("app exited unexpectedly. YarnState=" + applicationState.toString()
            + ". Exiting from client.");
        break;
      }

      if ((clientOpts.clientTimeout != -1) && (System.currentTimeMillis() > (clientStartTime
          + clientOpts.clientTimeout))) {
        LOG.info("Reached client specified timeout of " + clientOpts.clientTimeout
            + " ms for application. Killing application");
        break;
      }
      List<ApplicationAttemptReport> attempts = yarnClient.getApplicationAttempts(appId);
      for (ApplicationAttemptReport attempt : attempts) {
        ApplicationAttemptReport attemptReport = yarnClient
            .getApplicationAttemptReport(attempt.getApplicationAttemptId());
        YarnApplicationAttemptState attemptState = attemptReport
            .getYarnApplicationAttemptState();
        if (attemptState == YarnApplicationAttemptState.RUNNING) {
          ContainerId amContainerId = attemptReport.getAMContainerId();
          ContainerReport amReport = yarnClient.getContainerReport(amContainerId);
          String host = amReport.getAssignedNode().getHost();
          InetSocketAddress inetSocketAddress = new InetSocketAddress(host,
              DConstants.DEFAULT_AM_PROTOCOL_PORT);

          AmProtocol amClient = (AmProtocol) YarnRPC.create(getConf())
              .getProxy(AmProtocol.class, inetSocketAddress, getConf());
          AmStateResponse amState = amClient.getAMState();
          LOG.info("amClient getAMState:" + amState.getAMState());
        }
      }


    }
    return exitCode;
  }

  private void setupShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (yarnClient != null && appId != null) {
        try {
          ApplicationReport report = yarnClient.getApplicationReport(appId);
          YarnApplicationState state = report.getYarnApplicationState();
          if (state == YarnApplicationState.FAILED ||
              state == YarnApplicationState.FINISHED ||
              state == YarnApplicationState.KILLED) {
            LOG.info("application has been end state:" + state);
          } else {
            LOG.info("killing application " + appId.toString());
            yarnClient.killApplication(appId);
          }
        } catch (YarnException | IOException e) {
          LOG.error("kill application failed," + e.getMessage());
        }
      }
      LOG.info("Client stopped.");
    }));
  }

  private void setupApplication() throws YarnException, IOException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info(
        "Got Cluster metric info from ASM, numNodeManagers=" + clusterMetrics.getNumNodeManagers());

    QueueInfo queueInfo = yarnClient.getQueueInfo(clientOpts.amQueue);
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
    if (clientOpts.amMemory > maxMem || clientOpts.amMemory < 0
        || clientOpts.amVCores > maxVCores || clientOpts.amVCores < 0) {
      throw new IllegalArgumentException(
          "Invalid AM memory or vcores: memory=" + clientOpts.amMemory + ", vcores="
              + clientOpts.amVCores);
    }

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appId = appContext.getApplicationId();
    appContext.setApplicationName(clientOpts.appName);

    LOG.info("Setting up app master command");
    String command = setupAmCommand();
    List<String> commands = new ArrayList<String>();
    commands.add(command);
    LOG.info("Completed setting up app master command: " + command);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = setupRemoteResourcesGetEnv();

    // All of the resources for both AM and NN/DNs have been put on remote storage
    // Only the application master JAR is needed as a local resource for the AM so
    // we explicitly add it here
    Map<String, LocalResource> localResources = new HashMap<>();
    LocalResource scRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(DConstants.JAR_DEPENDENCIES.getPath(env)),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
        DConstants.JAR_DEPENDENCIES.getLength(env), DConstants.JAR_DEPENDENCIES.getTimestamp(env));
    localResources.put(DConstants.JAR_DEPENDENCIES.getResourcePath(), scRsrc);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, null, null);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Resource.newInstance(clientOpts.amMemory, clientOpts.amVCores);
    appContext.setResource(capability);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
      Credentials credentials = new Credentials();
      String tokenRenewer = getConf().get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
            "Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] =
          fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }

    appContext.setAMContainerSpec(amContainer);
    // Set the priority for the application master
    Priority pri = Priority.newInstance(clientOpts.amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(clientOpts.amQueue);
    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    yarnClient.submitApplication(appContext);
  }

  private Map<String, String> setupRemoteResourcesGetEnv() throws IOException {
    Map<String, String> env = new HashMap<>();
    // Copy local resources to a remote FS to prepare them for localization
    // by containers. We do not need to set them as local resources here as
    // the AM does not need them.
    setupRemoteResource(appId, DConstants.JAR_DEPENDENCIES, env, jarFile);
//    String log4jLocation = Client.class.getClassLoader().getResource("log4j.properties").toString();
//    setupRemoteResource(appId, DConstants.LOG4J, env, log4jLocation);

    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
        .append("./")
        .append(DConstants.JAR_DEPENDENCIES.getResourcePath())
        .append("/*");
    for (String c : getConf().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
    env.put(DConstants.REMOTE_STORAGE_PATH_ENV, getRemoteStoragePath().toString());

    env.put(Environment.CLASSPATH.key(), classPathEnv.toString());
    return env;
  }

  /**
   * Upload a local resource to HDFS, or if it is nonlocal, just set environment appropriately. The
   * location, length and timestamp information is added to AM container's environment, so it can
   * launch containers later with the correct resource settings.
   */
  private void setupRemoteResource(ApplicationId appId, DResource resource, Map<String, String> env,
      String... srcPaths) throws IOException {

    FileStatus remoteFileStatus;
    Path dstPath;

    Preconditions.checkArgument(srcPaths.length > 0, "Must supply at least one source path");
    Preconditions
        .checkArgument(resource.getType() == LocalResourceType.ARCHIVE || srcPaths.length == 1,
            "Can only specify multiple source paths if using an ARCHIVE type");

    List<URI> srcURIs = Arrays.stream(srcPaths).map(URI::create).collect(Collectors.toList());
    Set<String> srcSchemes = srcURIs.stream().map(URI::getScheme).collect(Collectors.toSet());
    Preconditions
        .checkArgument(srcSchemes.size() == 1, "All source paths must have the same scheme");
    String srcScheme = srcSchemes.iterator().next();

    String srcPathString = "[" + Joiner.on(",").join(srcPaths) + "]";

    if (srcScheme == null || srcScheme.equals(FileSystem.getLocal(getConf()).getScheme())
        || srcScheme.equals("jar")) {
      // Need to upload this resource to remote storage
      List<File> srcFiles = srcURIs.stream()
          .map(URI::getSchemeSpecificPart).map(File::new)
          .collect(Collectors.toList());
      Path dstPathBase = getRemoteStoragePath();
      boolean shouldArchive = srcFiles.size() > 1 || srcFiles.get(0).isDirectory()
          || (resource.getType() == LocalResourceType.ARCHIVE
          && Utils.isArchive(srcFiles.get(0).getName()));
      if (shouldArchive) {
        if ("jar".equals(srcScheme)) {
          throw new IllegalArgumentException(String.format(
              "Resources in JARs can't be auto-zipped; resource %s is ARCHIVE and src is: %s",
              resource.getResourcePath(), srcPathString));
        } else if (resource.getType() != LocalResourceType.ARCHIVE) {
          throw new IllegalArgumentException(String.format(
              "Resource type is %s but srcPaths were: %s", resource.getType(), srcPathString));
        }
        dstPath = new Path(dstPathBase, resource.getResourcePath()).suffix(".zip");
      } else {
        dstPath = new Path(dstPathBase, srcFiles.get(0).getName());
      }
      FileSystem remoteFS = dstPath.getFileSystem(getConf());
      LOG.info("Uploading resource " + resource + " from " + srcPathString + " to " + dstPath);
      try (OutputStream outputStream = remoteFS.create(dstPath, true)) {
        if ("jar".equals(srcScheme)) {
          try (InputStream inputStream = new URL(srcPaths[0]).openStream()) {
            IOUtils.copyBytes(inputStream, outputStream, getConf());
          }
        } else if (shouldArchive) {
          List<File> filesToZip;
          if (srcFiles.size() == 1 && srcFiles.get(0).isDirectory()) {
            File[] childFiles = srcFiles.get(0).listFiles();
            if (childFiles == null || childFiles.length == 0) {
              throw new IllegalArgumentException(
                  "Specified a directory to archive with no contents");
            }
            filesToZip = Lists.newArrayList(childFiles);
          } else {
            filesToZip = srcFiles;
          }
          ZipOutputStream zout = new ZipOutputStream(outputStream);
          for (File fileToZip : filesToZip) {
            addFileToZipRecursively(fileToZip.getParentFile(), fileToZip, zout);
          }
          zout.close();
        } else {
          try (InputStream inputStream = new FileInputStream(srcFiles.get(0))) {
            IOUtils.copyBytes(inputStream, outputStream, getConf());
          }
        }
      }
      remoteFileStatus = remoteFS.getFileStatus(dstPath);
    } else {
      if (srcPaths.length > 1) {
        throw new IllegalArgumentException(
            "If resource is on remote, must be a single file: " + srcPathString);
      }
      LOG.info("Using resource " + resource + " directly from current location: " + srcPaths[0]);
      dstPath = new Path(srcPaths[0]);
      // non-local file system; we can just use it directly from where it is
      remoteFileStatus = FileSystem.get(dstPath.toUri(), getConf()).getFileStatus(dstPath);
      if (remoteFileStatus.isDirectory()) {
        throw new IllegalArgumentException(
            "If resource is on remote filesystem, must be a file: " + srcPaths[0]);
      }
    }
    env.put(resource.getLocationEnvVar(), dstPath.toString());
    env.put(resource.getTimestampEnvVar(), String.valueOf(remoteFileStatus.getModificationTime()));
    env.put(resource.getLengthEnvVar(), String.valueOf(remoteFileStatus.getLen()));
  }

  private void addFileToZipRecursively(File root, File file, ZipOutputStream out)
      throws IOException {

    File[] files = file.listFiles();
    if (files == null) { // Not a directory
      String relativePath = file.getAbsolutePath().substring(
          root.getAbsolutePath().length() + 1);
      try {
        FileInputStream in = new FileInputStream(file.getAbsolutePath());
        out.putNextEntry(new ZipEntry(relativePath));
        IOUtils.copyBytes(in, out, getConf(), false);
        out.closeEntry();
        in.close();
      } catch (FileNotFoundException fnfe) {
        LOG.warn("Skipping file because it is a symlink with a nonexistent target: " + file);
      }
    } else {
      for (File containedFile : files) {
        addFileToZipRecursively(root, containedFile, out);
      }
    }
  }

  /**
   * Get the directory on the default FS which will be used for storing files relevant to this
   * Dynamometer application. This is inside of the {@value DConstants#REMOTE_STORAGE_DIR} directory
   * within the submitter's home directory.
   *
   * @return Fully qualified path on the default FS.
   */
  private Path getRemoteStoragePath() {
    if (StringUtils.isNotBlank(clientOpts.remoteStorage)) {
      return fs.makeQualified(new Path(clientOpts.remoteStorage, appId.toString()));
    }
    return fs.makeQualified(new Path(fs.getHomeDirectory(),
        DConstants.REMOTE_STORAGE_DIR + "/" + appId));
  }

  /**
   * set ApplicationMaster command
   */
  private String setupAmCommand() {
    List<String> vargs = new ArrayList<>();
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + clientOpts.amMemory + "m");
//    vargs.add("-agentlib:jdwp=transport=dt_socket,server=n,address=192.168.199.5:5005,suspend=y");
    // Set class name
    vargs.add(appMasterMainClass.getCanonicalName());
    // Set params for Application Master
    vargs.add("--container_mem " + String.valueOf(clientOpts.containerMemory));
    vargs.add("--container_vcores " + String.valueOf(clientOpts.containerVirtualCores));
    vargs.add("--container_priority " + String.valueOf(clientOpts.containerPriority));
    vargs.add("--num_container " + String.valueOf(clientOpts.numContainers));

    for (Map.Entry<String, String> entry : clientOpts.getShellEnv().entrySet()) {
      vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
    }
    if (clientOpts.debugFlag) {
      vargs.add("--debug");
    }

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final commmand
    return String.join(" ", vargs);
  }
}
