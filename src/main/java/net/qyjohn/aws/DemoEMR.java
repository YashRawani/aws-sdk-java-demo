package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;

public class DemoEMR 
{
	public AmazonElasticMapReduceClient client;
	final static Logger logger = Logger.getLogger(DemoEMR.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoEMR()
	{
		// Create the AmazonElasticMapReduceClient
		client = new AmazonElasticMapReduceClient();
		// Set the region to ap-southeast-2
		client.configureRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method launches an EMR cluster. You will need to modify the parameters in this 
	 * method to make it work.
	 *
	 */

	public void launchCluster()
	{
		System.out.println("\n\nLAUNCH CLUSTER\n\n");

		try
		{
			// Construct a RunJobFlowRequest.
			RunJobFlowRequest request = new RunJobFlowRequest();
			request.setAmiVersion("3.8.0");
			request.setName("Application Test 2");
			request.setServiceRole("EMR_DefaultRole");
			request.setJobFlowRole("EMR_EC2_DefaultRole");

			JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
			instances.setInstanceCount(new Integer(1));
			instances.setKeepJobFlowAliveWhenNoSteps(true);
			instances.setMasterInstanceType("m3.xlarge");
			instances.setSlaveInstanceType("m3.xlarge");
			instances.setEc2KeyName("desktop");
			request.setInstances(instances);

			SupportedProductConfig hive = new SupportedProductConfig();
			hive.setName("HIVE");
			SupportedProductConfig pig = new SupportedProductConfig();
			pig.setName("PIG");
			SupportedProductConfig hue = new SupportedProductConfig();
			hue.setName("HUE");
			SupportedProductConfig spark = new SupportedProductConfig();
			spark.setName("SPARK");
			SupportedProductConfig impala = new SupportedProductConfig();
			impala.setName("IMPALA");
			SupportedProductConfig ganglia = new SupportedProductConfig();
			ganglia.setName("GANGLIA");
			SupportedProductConfig hbase = new SupportedProductConfig();
			hbase.setName("HBASE");
			SupportedProductConfig hunk = new SupportedProductConfig();

			ArrayList<SupportedProductConfig> list = new ArrayList<SupportedProductConfig>();
			list.add(hive);
			list.add(pig);
			list.add(hue);
			list.add(spark);
			list.add(impala);
			list.add(ganglia);
			list.add(hbase);
			request.setNewSupportedProducts(list);

			// Launch the EMR cluster.
			RunJobFlowResult  result  = client.runJobFlow(request);
			String jobflowId = result.getJobFlowId();
			System.out.println("Launching cluster " + jobflowId);
		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 *
	 * Test method main().
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			DemoEMR demo = new DemoEMR();

			String action = args[0];
			if (action.equals("launch"))
			{
				demo.launchCluster();
			}
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
