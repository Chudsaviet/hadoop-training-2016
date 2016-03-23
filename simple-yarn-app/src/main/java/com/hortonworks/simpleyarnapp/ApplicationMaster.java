package com.hortonworks.simpleyarnapp;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;


import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {
    
  

  public static void main(String[] args) throws Exception {

    String command = args[0];
    final int n = Integer.valueOf(args[1]);
    final Path jarPath = new Path(args[2]);
    
    // Initialize clients to ResourceManager and NodeManagers
    Configuration conf = new YarnConfiguration();

    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    System.out.println("registerApplicationMaster 0");
    rmClient.registerApplicationMaster("", 0, "");
    System.out.println("registerApplicationMaster 1");
    
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager
    for (int i = 0; i < n; ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      System.out.println("Making res-req " + i);
      rmClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers, launch and check for responses
    int responseId = 0;
    int completedContainers = 0;
    while (completedContainers < n) {
        AllocateResponse response = rmClient.allocate(responseId++);
        for (Container container : response.getAllocatedContainers()) {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);

            // Calculate command
            command = "$JAVA_HOME/bin/java" +
                    " -Xmx256M" +
                    " com.hortonworks.simpleyarnapp.UrlContainer" +
                    " " + command +
                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

            System.out.println(" Setup jar for ApplicationMaster");
            LocalResource appMasterJar = Records.newRecord(LocalResource.class);
            setupAppMasterJar(jarPath, appMasterJar, conf);
            ctx.setLocalResources(
                Collections.singletonMap("simpleapp.jar", appMasterJar));

            System.out.println(" Setup CLASSPATH for ApplicationMaster");
            Map<String, String> appMasterEnv = new HashMap<String, String>();
            setupAppMasterEnv(appMasterEnv, conf);
            ctx.setEnvironment(appMasterEnv);

            ctx.setCommands(
                    Collections.singletonList(
                            command +
                                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    ));
            System.out.println("Launching container " + container.getId());
            nmClient.startContainer(container, ctx);
        }
        for (ContainerStatus status : response.getCompletedContainersStatuses()) {
            ++completedContainers;
            System.out.println("Completed container " + status.getContainerId());
        }
        Thread.sleep(100);
    }

    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
  }

  private static void setupAppMasterJar(Path jarPath, LocalResource appMasterJar, Configuration conf) throws IOException {
    FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  private static void setupAppMasterEnv(Map<String, String> appMasterEnv, Configuration conf) {
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");
  }
}

