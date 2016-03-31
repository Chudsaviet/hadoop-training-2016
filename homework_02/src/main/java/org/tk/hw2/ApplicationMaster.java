package org.tk.hw2;


import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

    public static void main(String[] args) throws Exception {

        String url_file = args[0];
        String output_path = args[1];
        final int n = Integer.valueOf(args[2]);
        final Path jarPath = new Path(args[3]);

        // Distribute work for containers
        List<String> container_inputs = distributeLines(output_path + "/work_distribution/", url_file, n);

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
        int containerNumber = 0;
        while (completedContainers < n) {
            AllocateResponse response = rmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);

                // Calculate command
                String command = "$JAVA_HOME/bin/java" +
                        " -Xmx256M" +
                        " org.tk.hw1.UrlContainer" +
                        " " + container_inputs.get(containerNumber) +
                        " " + output_path +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
                containerNumber++;

                System.out.println(" Setup jar for Container");
                LocalResource containerJar = Records.newRecord(LocalResource.class);
                setupContainerJar(jarPath, containerJar, conf);
                ctx.setLocalResources(
                        Collections.singletonMap("hw1.jar", containerJar));

                System.out.println(" Setup CLASSPATH for Container");
                Map<String, String> containerEnc = new HashMap<String, String>();
                setupContainerEnv(containerEnc, conf);
                ctx.setEnvironment(containerEnc);

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

    private static List<String> distributeLines(String out_path, String file, int n) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        Path in_path = new Path(file);
        BufferedReader in_reader = new BufferedReader(new InputStreamReader(fs.open(in_path)));
        int i = 0;

        List<String> name_list = new ArrayList<String>(n);
        List<PrintWriter> writer_list = new ArrayList<PrintWriter>(n);

        for (i = 0; i < n; i++) {
            String name = out_path + "/part_" + Integer.toString(i) + ".txt";
            name_list.add(name);

            Path writer_path = new Path(name);

            writer_list.add(new PrintWriter(fs.create(writer_path)));
        }

        i = 0;
        String line = in_reader.readLine(); //skip first line because it contains column names
        while (line != null) {
            line = in_reader.readLine();

            writer_list.get(i).println(line);

            i++;
            if (i >= n) i = 0;
        }

        for (i = 0; i < n; i++) {
            writer_list.get(i).close();
        }

        fs.close();
        return name_list;
    }

    private static void setupContainerJar(Path jarPath, LocalResource containerJar, Configuration conf) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        containerJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        containerJar.setSize(jarStat.getLen());
        containerJar.setTimestamp(jarStat.getModificationTime());
        containerJar.setType(LocalResourceType.FILE);
        containerJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private static void setupContainerEnv(Map<String, String> containerEnc, Configuration conf) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(containerEnc, Environment.CLASSPATH.name(),
                    c.trim());
        }
        Apps.addToEnvironment(containerEnc,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*");
    }
}

