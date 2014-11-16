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
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.PutNotificationConfigurationRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
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
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;


public class myAutoScaling {
    public static void main(String[] args) throws Exception {
        System.out.println("Sure you want to run me?(in case of mis-run)");
        //Thread.sleep(10 * 1000);
        
        // initialization, set up ec2 client
        AmazonEC2Client ec2 = init_ec2();
        
        // create an Elastic Load Balancer (ELB) instance
        AmazonElasticLoadBalancingClient elb = init_elb();
        String elbDns = CreateELB(elb);
        
        // set up the AWS Auto Scaling
        AmazonAutoScalingClient as = init_as_cw();
        

        // update AutoScaling group for warm-up
        UpdateAutoScalingGroupRequest update_req = new UpdateAutoScalingGroupRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withMinSize(5)
        .withMaxSize(5)
        .withDesiredCapacity(5);
        
        as.updateAutoScalingGroup(update_req);
        
        // create the load generator by spot instance
        System.out.println("Creating Load Generator...");
        
        Instance loadGenerator = null;
        if ((loadGenerator = CreatLoadGenerator(ec2)) == null) {
            System.err.println("Spot Instance Creating Failed!");
            return;
        }
        String loadGeneratorDNS = loadGenerator.getPublicDnsName();
        
        // activate load generator
        activateInst(loadGeneratorDNS);
        Thread.sleep(15 * 1000);
        
        // warm up elb for given times
        for (int i = 0; i < 4; i++)
            Warmup(loadGeneratorDNS, elbDns, "warm"+i);
        
        // update the max size of  AutoScaling group
        UpdateAutoScalingGroupRequest min_req = new UpdateAutoScalingGroupRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withDesiredCapacity(5)
        .withMaxSize(8)
        .withMinSize(5);
        as.updateAutoScalingGroup(min_req);
        
        SetScalingPolicy(as);
        
        Thread.sleep(10 * 1000);
        // phase 3 test
        beginPhase3(loadGeneratorDNS, elbDns, "goodluck");
        

        System.out.println("WARNING: the clean up will start in 5 minutes, pls store your result!");
        Thread.sleep(5 * 60 * 1000);
        System.out.println("Starting clean up!");
        
        // clean up - delete all the instances
        UpdateAutoScalingGroupRequest cleanup_req = new UpdateAutoScalingGroupRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withMinSize(0)
        .withMaxSize(0)
        .withDesiredCapacity(0);
        
        as.updateAutoScalingGroup(cleanup_req);
        
        TerminateInstancesRequest del_inst_req = new TerminateInstancesRequest()
        .withInstanceIds(loadGenerator.getInstanceId());
        ec2.terminateInstances(del_inst_req);
        
        Thread.sleep(3*60*1000);
        System.out.println("Instances deleted!");
        
        // delete auto scaling group
        DeleteAutoScalingGroupRequest del_req = new DeleteAutoScalingGroupRequest()
        .withAutoScalingGroupName("DataCenterGroup");
        as.deleteAutoScalingGroup(del_req);
        Thread.sleep(60*1000);
        System.out.println("Auto scaling group deleted!");
        
        // delete launch configuration
        DeleteLaunchConfigurationRequest del_lau_req = new DeleteLaunchConfigurationRequest()
        .withLaunchConfigurationName("ccDataCenter");
        as.deleteLaunchConfiguration(del_lau_req);
        System.out.println("Launch configuration deleted!");
        
        DeleteLoadBalancerRequest del_elb_req = new DeleteLoadBalancerRequest()
        .withLoadBalancerName("greatELB");
        elb.deleteLoadBalancer(del_elb_req);
        System.out.println("Load balancer deleted!");
        
        System.out.println("Project 2.3 Finished! Have a good day, bye!");
    }
    
    // start the warm-up
    public static void Warmup(String loadgen_dns, String elb_dns, String testID) {
        String url = "http://" + loadgen_dns + "/warmup";
        String param = "dns=" + elb_dns + "&testId=" + testID;
        
        while (true) {
            try {
                sendGet(url, param);
                break;
            } catch (Exception e) {
                System.out.println("Submission Failed: server not ready. Have a new try!");
                try {
                      Thread.sleep(5 * 1000);
                } catch (Exception e2) {
                    // do nothing
                }
                continue;
            }
        }
        System.out.println("Warmup submitted!");
        url = "http://" + loadgen_dns + "/view-logs?name=warmup_jiachenl.txt";
        System.out.println("Pls go to " + url + " for details");
        try {
            Thread.sleep((6*60)*1000); // sleep 5.5 min
        } catch (Exception e2) {
            // do nothing
        }
        
        System.out.println("Warmup finished!");
    }
    
    // start the phase 2, and sleep for 42 min
    public static void beginPhase3(String loadgen_dns, String elb_dns, String testID) {
        String url = "http://" + loadgen_dns + "/begin-phase-3";
        String param = "dns=" + elb_dns + "&testId=" + testID;
        
        while (true) {
            try {
                sendGet(url, param);
                break;
            } catch (Exception e) {
                System.out.println("Submission Failed: server not ready. Have a new try!");
                try {
                      Thread.sleep(5 * 1000);
                } catch (Exception e2) {
                    // do nothing
                }
                continue;
            }
        }
        System.out.println("Phase 2 has started!");
        url = "http://" + loadgen_dns + "/view-logs?name=result_jiachenl_" + testID + ".txt";
        System.out.println("Pls go to " + url + " for details");
        try {
            Thread.sleep(105*60*1000);
        } catch (Exception e2) {
            // do nothing
        }
        
        System.out.println("Phase 2 finished!");
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
                    Thread.sleep(8 * 1000);
                } catch (Exception e2) {
                    // do nothing
                }
                continue;
            }
        }
        System.out.println("Activation sucess!");
    }
    
    // send a GET to the server (low level function)
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
    public static AmazonEC2Client init_ec2() throws IOException{
        //Load the Properties File with AWS Credentials
        Properties properties = new Properties();
        properties.load(myCCProj.class.getResourceAsStream("/AwsCredentials.properties"));
        
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), 
                properties.getProperty("secretKey"));
        //Create an Amazon EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
        ec2.setRegion(Region.getRegion(Regions.US_EAST_1)); // in US East N. Virginia region

        System.out.println("EC2 Client Created!");
        return ec2;
    }
    
    // initialization for ELB client
    public static AmazonElasticLoadBalancingClient init_elb() throws IOException {
        //Load the Properties File with AWS Credentials, same as ec2
        Properties properties = new Properties();
        properties.load(myCCProj.class.getResourceAsStream("/AwsCredentials.properties"));
        
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), 
                properties.getProperty("secretKey"));
        
        AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(bawsc);
        elb.setRegion(Region.getRegion(Regions.US_EAST_1)); // in US East N. Virginia region
        
        System.out.println("ELB Client Created!");
        return elb;
    }
    
    // initialization and configuration for AWS Auto Scaling Service
    public static AmazonAutoScalingClient init_as_cw() throws IOException {
        //Load the Properties File with AWS Credentials, same as ec2
        Properties properties = new Properties();
        properties.load(myCCProj.class.getResourceAsStream("/AwsCredentials.properties"));
        
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), 
                properties.getProperty("secretKey"));
        
        AmazonAutoScalingClient as = new AmazonAutoScalingClient(bawsc);
        
        as.setRegion(Region.getRegion(Regions.US_EAST_1));
        
        // set up launch configuration
        CreateLaunchConfigurationRequest lcReq = new CreateLaunchConfigurationRequest()
        .withLaunchConfigurationName("ccDataCenter")
        .withImageId("ami-3c8f3a54")
        .withInstanceType("m1.small")
        .withSecurityGroups("sg-eb87d58e")
        .withKeyName("project2")
        .withInstanceMonitoring(new InstanceMonitoring().withEnabled(true));
        
        as.createLaunchConfiguration(lcReq);
        
        
        // set tag info
        com.amazonaws.services.autoscaling.model.Tag tag = new com.amazonaws.services.autoscaling.model.Tag()
        .withKey("Project")
        .withValue("2.3");

        // create AutoScaling group
        CreateAutoScalingGroupRequest asgReq = new CreateAutoScalingGroupRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withLaunchConfigurationName("ccDataCenter") // as above
        .withLoadBalancerNames("greatELB")
        .withHealthCheckType("ELB")
        .withHealthCheckGracePeriod(30)
        .withDefaultCooldown(180)
        .withTags(tag)
        .withAvailabilityZones("us-east-1b")
        .withMinSize(0)              // 3: disabling it for the moment
        .withMaxSize(0)              // 5: disabling it for the moment
        .withDesiredCapacity(0);     // 3: will configure at main()
        
        as.createAutoScalingGroup(asgReq);
        
        // notification
        //as.describeNotificationConfigurations()
        PutNotificationConfigurationRequest sns_req = new PutNotificationConfigurationRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withNotificationTypes("autoscaling:EC2_INSTANCE_LAUNCH", "autoscaling:EC2_INSTANCE_TERMINATE",
                "autoscaling:EC2_INSTANCE_LAUNCH_ERROR", "autoscaling:EC2_INSTANCE_TERMINATE_ERROR")
        .withTopicARN("arn:aws:sns:us-east-1:466336390159:CCProject");
        
        as.putNotificationConfiguration(sns_req);
        
        
        
        System.out.println("AWS Auto Scaling Group 'DataCenterGroup' has been created!");        
        return as;
    }

    public static void SetScalingPolicy(AmazonAutoScalingClient as) throws IOException {
        
        Properties properties = new Properties();
        properties.load(myCCProj.class.getResourceAsStream("/AwsCredentials.properties"));
        
        BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), 
                properties.getProperty("secretKey"));
        
        AmazonCloudWatchClient cw = new AmazonCloudWatchClient(bawsc);

        cw.setRegion(Region.getRegion(Regions.US_EAST_1));
        
        // Scaling Policy and Alarm
        // Quick up policy
        PutScalingPolicyRequest reqQuickUp = new PutScalingPolicyRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withPolicyName("DDos-quick-up")  // This scales up so I've put up at the end. 
        .withScalingAdjustment(2)         // scale out by 2 instance each time
        .withAdjustmentType("ChangeInCapacity");
        
        // Slow up policy
        //PutScalingPolicyRequest reqSlowUp = new PutScalingPolicyRequest()
        //.withAutoScalingGroupName("DataCenterGroup")
        //.withPolicyName("DDoS-slow-up")  // This scales up so I've put up at the end. 
        //.withScalingAdjustment(1)        // scale out by one
        //.withAdjustmentType("ChangeInCapacity");
        
        
        PutScalingPolicyRequest reqQuickDown = new PutScalingPolicyRequest()
        .withAutoScalingGroupName("DataCenterGroup")
        .withPolicyName("DDos-quick-down")  // This scales up so I've put up at the end. 
        .withScalingAdjustment(-1)          // scale in by one
        .withAdjustmentType("ChangeInCapacity");

        PutScalingPolicyResult rQuickUp = as.putScalingPolicy(reqQuickUp);
        String arnQuickUp = rQuickUp.getPolicyARN();
        

        Dimension dimension = new Dimension()
        .withName("AutoScalingGroupName")
        .withValue("DataCenterGroup");
        
        // Scale Out - quick if Average InternetIn > 30,000,000 for 1 min
        PutMetricAlarmRequest quickUpReq = new PutMetricAlarmRequest()
        .withAlarmName("Scale-out")
        .withMetricName("NetworkIn")
        .withNamespace("AWS/EC2")
        .withComparisonOperator(ComparisonOperator.GreaterThanThreshold)
        .withStatistic(Statistic.Sum)
        .withUnit(StandardUnit.Bytes)
        .withThreshold(105000000d)
        .withPeriod(60)
        .withEvaluationPeriods(1)
        .withAlarmActions(arnQuickUp)
        .withDimensions(dimension);

        cw.putMetricAlarm(quickUpReq);
        
        // Scale In - quick if Average InternetIn < 30,000,000 for 3 min
        PutScalingPolicyResult rQuickDown = as.putScalingPolicy(reqQuickDown);
        String arnQuickDown = rQuickDown.getPolicyARN();
        
        PutMetricAlarmRequest quickDownReq = new PutMetricAlarmRequest()
        .withAlarmName("Scale-in")
        .withMetricName("NetworkIn")
        .withNamespace("AWS/EC2")
        .withComparisonOperator(ComparisonOperator.LessThanThreshold)
        .withStatistic(Statistic.Sum)
        .withUnit(StandardUnit.Bytes)
        .withThreshold(105000000d)
        .withPeriod(60)
        .withEvaluationPeriods(2)
        .withAlarmActions(arnQuickDown)
        .withDimensions(dimension);

        cw.putMetricAlarm(quickDownReq);
    }
    
    public static String CreateELB(AmazonElasticLoadBalancingClient elb) {
        // create load balancer
        
        // configure listener
        Listener listener = new Listener()
        .withInstanceProtocol("HTTP")
        .withInstancePort(80)
        .withProtocol("HTTP")
        .withLoadBalancerPort(80);
        
        // create load balancer request
        CreateLoadBalancerRequest lbReq = new CreateLoadBalancerRequest()
        .withLoadBalancerName("greatELB")
        .withSubnets("subnet-0bfde923")
        .withListeners(listener);
        
        // health check configuration
        HealthCheck healthCheck = new HealthCheck()
        .withHealthyThreshold(2)
        .withInterval(10)
        .withTarget("HTTP:80/heartbeat?username=jiachenl")
        .withTimeout(5)
        .withUnhealthyThreshold(3);
        
        ConfigureHealthCheckRequest healthCheckReq = new ConfigureHealthCheckRequest()
        .withHealthCheck(healthCheck)
        .withLoadBalancerName("greatELB");
        
        // set the tag
        com.amazonaws.services.elasticloadbalancing.model.Tag elb_tag = new com.amazonaws.services.elasticloadbalancing.model.Tag();
        elb_tag.setKey("Project");
        elb_tag.setValue("2.3");
        lbReq.withTags(elb_tag);
        
        CreateLoadBalancerResult lbResult = elb.createLoadBalancer(lbReq);
        /*ConfigureHealthCheckResult healthCkResult = */elb.configureHealthCheck(healthCheckReq);
        
        String dns = lbResult.getDNSName();
        
        System.out.println("ELB with dns:" + dns + " has been created!");
        return dns;
    }

    // create a on-demand instance of load generator, in case that the spot
    // instance is not available.
    public static Instance CreatLoadGenerator(AmazonEC2Client ec2) throws Exception{
        // Create Instance Request - for Data Center
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
         
        // Configure Instance Request
        runInstancesRequest.withImageId("ami-7aba0c12")
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
        createTagsRequest.withResources(dc.getInstanceId()).withTags(new Tag("Project", "2.3"));
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
                        System.out.println("Waiting load generator (dns:" + instance.getPublicDnsName() + ") to be ready...");
                        Thread.sleep(30 * 1000);
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
        requestRequest.setSpotPrice("0.025");
        requestRequest.setInstanceCount(Integer.valueOf(1));

        // Setup the specifications of the launch. This includes the instance type (e.g. t1.micro)
        // and the latest Amazon Linux AMI id available. Note, you should always use the latest
        // Amazon Linux AMI id or another of your choosing.
        LaunchSpecification launchSpecification = new LaunchSpecification();
        launchSpecification.setImageId("ami-7aba0c12"); // load generator
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
                    createTagsRequest.withResources(myInstID).withTags(new Tag("Project", "2.3"));
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
                        System.out.println("Waiting load generator (dns:" + instance.getPublicDnsName() + ") to be ready...");
                        Thread.sleep(30 * 1000);
                        return instance;
                    }
                }
            }
            return null;
        }
    }

    
    
}
