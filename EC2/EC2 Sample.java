/**
 * @author Jiachen Li
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.Tag;


public class myEC2 {
    
    public static void main(String[] args) throws Exception {        
        
        // initialization, set up ec2 client
        AmazonEC2Client ec2 = init();

        // creat the load generator by spot instance
        // I have also implemented a on-demand version, i.e., CreatLoadGenerator(ec2)
        System.out.println("Creating Load Generator...");
        Instance loadGenerator;
        if ((loadGenerator = CreatSpotLoadGenerator(ec2)) == null) {
            System.err.println("Spot Instance Creating Failed!");
            return;
        }
        String loadGeneratorDNS = loadGenerator.getPublicDnsName();
        System.out.println("Waiting load generator to be ready...");
        Thread.sleep(30 * 1000);

        // send request to url, I have implemented some mechanism to 
        activateInst(loadGeneratorDNS);
        
		// array for storing the dns:rps pairs
        ArrayList<String> dataCenterDns = new ArrayList<String>();
        ArrayList<Double> dataCenterRps = new ArrayList<Double>();
        
        System.out.println("Start 1st Data Center!");
        Instance dc;
        if ((dc= CreatDataCenter(ec2)) == null) {
            System.out.println("Data Center Creating Failed!");
        }
        // sleep to wait for http ready, just for sure
        System.out.println("Waiting 1st Data Center to be ready...");
        Thread.sleep(30 * 1000);
        String dataDns = dc.getPublicDnsName();
        System.out.println("new Data Center ready. DNS: " + dataDns);
        activateInst(dataDns);
        Thread.sleep(3 * 1000);
        
        // add the first data center
        long lastAddingTime = System.currentTimeMillis();
        dataCenterDns.add(dataDns);
        dataCenterRps.add(0.0);
        
        // start the test with given ID
        String testID = "nice";
        submitDataCenter(loadGeneratorDNS, dataDns, testID);

        // set log url and start auto-scale
        String logUrl = "http://" + loadGeneratorDNS + "/view-logs?name=result_jiachenl_" + testID + ".txt";
        System.out.println("Waiting for log to be ready...");
        Thread.sleep(5 * 1000);
        
        // variables for parsing the log
        double sumRps = 0; // accumulated rps
        // minute past & the number of added data center
        int time = 0, count = 0;
        
        CHECKRESULTS:
        while (sumRps < 3600 && time <= 40) {
            String tmp;
            try {
                tmp = getResults(logUrl);
            } catch (Exception e) {
                continue CHECKRESULTS;
            }
             
            String[] lines = tmp.split("\n");
            
            // parse the lines
            for (int i = 0; i < lines.length; i++) {
                if (lines[i].startsWith("RESULT")) {
                    System.out.println(lines[i]);
                    System.out.println(lines[i+1]);
                    break CHECKRESULTS;
                }
                
                if (lines[i].startsWith("minute " + (time+1))) {
                    time++;
                    i++;
                    count = 0;
                    // for each line after minute
                    while (!lines[i].startsWith("---")) {
                        // for each line before dash
                        for (int j = 0; j < dataCenterDns.size(); j++) {
                            if (lines[i].startsWith(dataCenterDns.get(j))) {
                                String[] part = lines[i].split(":");
                                part[2] = part[2].trim();
                                String[] info = part[2].split(" ");
                                dataCenterRps.set(j, Double.parseDouble(info[0]));
                                count++;
                                break;
                            }
                        }
                        i++;
                    }
                }
            }
            
            // count the result and make the decision
            sumRps = 0;
            for (double rps : dataCenterRps) {
                sumRps += rps;
            }
            System.out.println("Accumulated rps at minute " + time + " is " + sumRps);
            
            // decide whether to add another data center
            long currentTime = System.currentTimeMillis();
            // add an another data center iff
            // 1. all the added data centers are shown in the results
            // 2. 2 min has past since last adding
            if (count == dataCenterDns.size() && currentTime - lastAddingTime > 2*60*1000) {
                System.out.println("decided to add a new data center!");
                Instance dtct;
                if ((dtct= CreatDataCenter(ec2)) == null) {
                     System.out.println("Data Center Creating Failed!");
                     continue;
                }
                System.out.println("new Data Center ready. DNS: " + dtct.getPublicDnsName());
                lastAddingTime = System.currentTimeMillis();
                
                // sleep to wait for http ready, just for sure
                System.out.println("Waiting for new Data Center to be ready...");
                Thread.sleep(30 * 1000);
                
                activateInst(dtct.getPublicDnsName());
                Thread.sleep(5 * 1000);
                
                submitDataCenter(loadGeneratorDNS, dtct.getPublicDnsName(), testID);

                // register in local set
                dataCenterDns.add(dtct.getPublicDnsName());
                dataCenterRps.add(0.0);
                System.out.println("New Data Center successfully loaded");
            }            
            
            Thread.sleep(9 * 1000);
            System.out.println();
        }
        
        // keep running
        System.out.println("Great, my task is over, pls finish your project and close all the instances. Bye!");

    }
    
    // get the log result
    public static String getResults(String url) throws Exception{  
        URL realUrl;
        StringBuffer strBuff = new StringBuffer();
        
        realUrl = new URL(url);
        BufferedReader in = new BufferedReader(new InputStreamReader(realUrl.openStream()));
        
        String line;
        while ((line = in.readLine()) != null)
            strBuff.append(line + "\n");

        in.close();
        
        return strBuff.toString();
    }
    
    // submit the data center to load generator, can submit multiple times to ensure success
    public static void submitDataCenter(String load_dns, String data_dns, String testID) {
        String url = "http://" + load_dns + "/part/one/i/want/more";
        String param = "dns=" + data_dns + "&testId=" + testID;
        
        while (true) {
            try {
                sendGet(url, param);
                break;
            } catch (Exception e) {
                System.out.println("Submission Failed: server not ready. Have a new try!");
                try {
                      Thread.sleep(3 * 1000);
                } catch (Exception e2) {
                    // do nothing
                }
                continue;
            }
        }
        System.out.println("Data center submission sucess!");
    }
    
    // activate the given instance, can do multiple times to ensure success
    public static void activateInst(String dns) {
        String url = "http://" + dns + "/username";
        String param = "username=jiachenl";
        
        while (true) {
            try {
                sendGet(url, param);
                break;
            } catch (Exception e) {
                System.out.println("Activation failed: server not ready. Have a new try!");
                try {
                    Thread.sleep(3 * 1000);
                } catch (Exception e2) {
                    // do nothing
                }
                continue;
            }
        }
        System.out.println("Activation sucess!");
    }
    
    // send a GET to the server
    public static String sendGet(String url, String param) throws Exception {
        String result = "";
        BufferedReader in = null;
        
        String urlReq = url + "?" + param;
        URL realUrl = new URL(urlReq);
        // open connection
        URLConnection conn = realUrl.openConnection();

        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("user-agent", "Mozilla/5.0");

        // build the connection
        conn.connect();

        Map<String, List<String>> map = conn.getHeaderFields();
        // print out the header
        for (String key : map.keySet()) {
            System.out.println(key + ":" + map.get(key));
        }

        // get the response
        in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            result += "\n" + line;
        }

        if (in != null)
            in.close();

        return result;
    }
    
    // initialization, i.e. credentials, EC2client
    public static AmazonEC2Client init() throws IOException{
        //Load the Properties File with AWS Credentials
        Properties properties = new Properties();
        properties.load(myEC2.class.getResourceAsStream("/AwsCredentials.properties"));
        
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), 
                properties.getProperty("secretKey"));
        //Create an Amazon EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
        ec2.setRegion(Region.getRegion(Regions.US_EAST_1)); // in US East N. Virginia region
        return ec2;
    }
    
    // create a on-demand instance of data center
    public static Instance CreatDataCenter(AmazonEC2Client ec2) {
        // Create Instance Request - for Data Center
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
         
        // Configure Instance Request
        runInstancesRequest.withImageId("ami-324ae85a")
        .withInstanceType("m3.medium")
        .withMinCount(1)
        .withMaxCount(1)
        .withKeyName("project2")
        .withSubnetId("subnet-0bfde923");
        
        // Launch Instance
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);  
         
        // Return the Object Reference of the Instance just Launched
        Instance dc = runInstancesResult.getReservation().getInstances().get(0);
        
        // add the tag to the Instance
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(dc.getInstanceId()).withTags(new Tag("Project", "2.1"));
        ec2.createTags(createTagsRequest);
        
        // wait for instance status to change
        WAITINGFORSTATUSCHANGE:
        while (true) {
            List<Reservation> reservations = ec2.describeInstances().getReservations();
            for(int i = 0; i < reservations.size(); i++) {
                List<Instance> instances = reservations.get(i).getInstances();
             
                int instanceCount = instances.size();
                
                // find the required instance
                for(int j = 0; j < instanceCount; j++) {
                    Instance instance = instances.get(j);
             
                    if(instance.getInstanceId().equals(dc.getInstanceId())) {
                        // find the load generator's instance
                        if (!instance.getState().getName().equals("running")) {
                            System.out.println("Current Status: " + instance.getState().getName());
                            try {
                                Thread.sleep(5 * 1000);
                                continue WAITINGFORSTATUSCHANGE;
                            } catch (Exception e) {
                                // awake early, restart
                                continue WAITINGFORSTATUSCHANGE;
                            }                        
                        }
                        return instance;
                    }
                }
            }
            return null;
        }
    }

    // create a on-demand instance of load generator, in case that the spot
    // instance is not available.
    public static Instance CreatLoadGenerator(AmazonEC2Client ec2) {
        // Create Instance Request - for Data Center
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
         
        // Configure Instance Request
        runInstancesRequest.withImageId("ami-1810b270")
        .withInstanceType("m3.medium")
        .withMinCount(1)
        .withMaxCount(1)
        .withKeyName("project2")
        .withSubnetId("subnet-0bfde923");
        
        // Launch Instance
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);  
         
        // Return the Object Reference of the Instance just Launched
        Instance dc = runInstancesResult.getReservation().getInstances().get(0);
        
        // add a tag to the Instance
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(dc.getInstanceId()).withTags(new Tag("Project", "2.1"));
        ec2.createTags(createTagsRequest);
        
        // wait for instance status to change
        WAITINGFORSTATUSCHANGE:
        while (true) {
            List<Reservation> reservations = ec2.describeInstances().getReservations();
            for(int i = 0; i < reservations.size(); i++) {
                List<Instance> instances = reservations.get(i).getInstances();
             
                int instanceCount = instances.size();
                
                // find the required instance
                for(int j = 0; j < instanceCount; j++) {
                    Instance instance = instances.get(j);
             
                    if(instance.getInstanceId().equals(dc.getInstanceId())) {
                        // find the load generator's instance
                        if (!instance.getState().getName().equals("running")) {
                            System.out.println("Current Status: " + instance.getState().getName());
                            try {
                                Thread.sleep(5 * 1000);
                                continue WAITINGFORSTATUSCHANGE;
                            } catch (Exception e) {
                                // awake early, restart
                                continue WAITINGFORSTATUSCHANGE;
                            }                        
                        }
                        return instance;
                    }
                }
            }
            return null;
        }
    }

    // create a spot instance of load generator, built on Amazon EC2 JDK
    public static Instance CreatSpotLoadGenerator(AmazonEC2Client ec2) throws Exception {        
        // Initializes a Spot Instance Request
        RequestSpotInstancesRequest requestRequest = new RequestSpotInstancesRequest();

        // Request 1 x m3.medium instance with a bid price
        requestRequest.setSpotPrice("0.012");
        requestRequest.setInstanceCount(Integer.valueOf(1));

        // Setup the specifications of the launch. This includes the instance type (e.g. t1.micro)
        // and the latest Amazon Linux AMI id available. Note, you should always use the latest
        // Amazon Linux AMI id or another of your choosing.
        LaunchSpecification launchSpecification = new LaunchSpecification();
        launchSpecification.setImageId("ami-1810b270"); // load generator
        launchSpecification.setInstanceType("m3.medium");
        launchSpecification.withKeyName("project2");
        launchSpecification.withSubnetId("subnet-0bfde923");

        // Add the launch specifications to the request.
        requestRequest.setLaunchSpecification(launchSpecification);
        
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResult requestResult = ec2.requestSpotInstances(requestRequest);
        
        // Getting the Request ID from the Request
        List<SpotInstanceRequest> requestResponses = requestResult.getSpotInstanceRequests();

        // Setup an arraylist to collect all of the request ids we want to watch hit the running
        // state.
        ArrayList<String> spotInstanceRequestIds = new ArrayList<String>();

        // Add all of the request ids to the hashset, so we can determine when they hit the
        // active state.
        for (SpotInstanceRequest requestResponse : requestResponses) {
            System.out.println("Created Spot Request: "+requestResponse.getSpotInstanceRequestId());
            spotInstanceRequestIds.add(requestResponse.getSpotInstanceRequestId());
        }
        

        // Determining the State of the Spot Request
        
        // Create a variable that will track whether there are any requests still in the open state.
        boolean anyOpen;

        // Initialize variables.
        ArrayList<String> instanceIds = new ArrayList<String>();
        String myInstID = null; // the instance that I created

        do {
            // Create the describeRequest with tall of the request id to monitor (e.g. that we started).
            DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
            describeRequest.setSpotInstanceRequestIds(spotInstanceRequestIds);

            // Initialize the anyOpen variable to false ??? which assumes there are no requests open unless
            // we find one that is still open.
            anyOpen = false;

            try {
                // Retrieve all of the requests we want to monitor.
                DescribeSpotInstanceRequestsResult describeResult = ec2.describeSpotInstanceRequests(describeRequest);
                List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();

                // Look through each request and determine if they are all in the active state.
                for (SpotInstanceRequest describeResponse : describeResponses) {
                    // If the state is open, it hasn't changed since we attempted to request it.
                    // There is the potential for it to transition almost immediately to closed or
                    // cancelled so we compare against open instead of active.
                    if (describeResponse.getState().equals("open")) {
                        anyOpen = true;
                        break;
                    }
                    
                    // Add the instance id to the list we will eventually terminate.
                    instanceIds.add(describeResponse.getInstanceId());
                    myInstID = describeResponse.getInstanceId();
                    
                    // Add a Tag to the Instance
                    CreateTagsRequest createTagsRequest = new CreateTagsRequest();
                    createTagsRequest.withResources(myInstID).withTags(new Tag("Project", "2.1"));
                    ec2.createTags(createTagsRequest);
                    
                    System.out.println("Instance(ID:" + myInstID + ") is active now!");
                }
            } catch (AmazonServiceException e) {
                // If we have an exception, ensure we don't break out of the loop.
                // This prevents the scenario where there was blip on the wire.
                anyOpen = true;
            }
            
            if (anyOpen)
                System.out.println("Spot instance is still pending evaluation..");

            try {
                // Sleep for 10 seconds each time
                if (anyOpen)
                    Thread.sleep(10 * 1000);
            } catch (Exception e) {
                // Do nothing because it woke up early.
            }
        } while (anyOpen);
        
        // waiting for instance status to change
        WAITINGFORSTATUSCHANGE:
        while (true) {
            List<Reservation> reservations = ec2.describeInstances().getReservations();
            for(int i = 0; i < reservations.size(); i++) {
                List<Instance> instances = reservations.get(i).getInstances();
                int instanceCount = instances.size();
                
                // find the required instance
                for(int j = 0; j < instanceCount; j++) {
                    Instance instance = instances.get(j);
             
                    if(instance.getInstanceId().equals(myInstID)) {
                        // find the load generator's instance
                        if (!instance.getState().getName().equals("running")) {
                            System.out.println("Current Status: " + instance.getState().getName());
                            try {
                                Thread.sleep(8 * 1000);
                                continue WAITINGFORSTATUSCHANGE;
                            } catch (Exception e) {
                                // awake early, restart
                                continue WAITINGFORSTATUSCHANGE;
                            }                        
                        }
                        return instance;
                    }
                }
            }
            return null;
        }
    }
    
}
