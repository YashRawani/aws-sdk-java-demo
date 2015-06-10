package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.ec2.*;
import com.amazonaws.services.ec2.model.*;

public class DemoEC2 
{
	public AmazonEC2Client client;
//	final static Logger logger = Logger.getLogger(DemoEC2.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoEC2()
	{
		// Create the AmazonEC2Client
		client = new AmazonEC2Client();
		// Set the region to ap-southeast-2
		client.setRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method launches an EC2 instance. You will need to modify the parameters in this 
	 * method to make it work.
	 *
	 */

	public String launchInstance()
	{
		System.out.println("\n\nLAUNCH INSTANCE\n\n");

		try
		{
			// Construct a RunInstancesRequest.
			RunInstancesRequest request = new RunInstancesRequest();
			request.setImageId("ami-fd9cecc7");	// the AMI ID, ami-fd9cecc7 is Amazon Linux AMI 2015.03 (HVM)
			request.setInstanceType("t2.micro");	// instance type
			request.setKeyName("desktop");		// the keypair
			request.setSubnetId("subnet-2dc0d459");	// the subnet
			ArrayList<String> list = new ArrayList<String>();
			list.add("sg-efcc248a");			// security group, call add() again to add more than one
			request.setSecurityGroupIds(list);
			request.setMinCount(1);	// minimum number of instances to be launched
			request.setMaxCount(1);	// maximum number of instances to be launched

			// Pass the RunInstancesRequest to EC2.
			RunInstancesResult  result  = client.runInstances(request);
			String instanceId = result.getReservation().getInstances().get(0).getInstanceId();
			
			// Return the first instance id in this reservation.
			// So, don't launch multiple instances with this demo code.
			System.out.println("Launching instance " + instanceId);
			return instanceId;
		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
			return "ERROR";
		}
	}


	/**
	 *
	 * This method terminates an EC2 instance. The parameters include
	 * @param instanceId
	 *
	 */

	public void terminateInstance(String instanceId)
	{
		System.out.println("\n\nTERMINATE INSTANCE\n\n");
		try
		{
			// Construct the TerminateInstancesRequest
			TerminateInstancesRequest request = new TerminateInstancesRequest();
			ArrayList<String> list = new ArrayList<String>();
			list.add(instanceId);			// instance id
			request.setInstanceIds(list);

			// Pass the TerminateInstancesRequest to EC2
			TerminateInstancesResult result = client.terminateInstances(request);
			List <InstanceStateChange> changes = result.getTerminatingInstances();
			for (InstanceStateChange change : changes)
			{
				String id = change.getInstanceId();
				String state_prev = change.getPreviousState().toString();
				String state_next = change.getCurrentState().toString();
				System.out.println("Instance " + id + " is changing from " + state_prev + " to " + state_next + ".");
			}
		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 *
	 * This method lists all the EC2 instances in a region.
	 *
	 */

	public void listInstances()
	{
		System.out.println("\n\nLIST INSTANCE\n\n");
        	try 
		{
			// DescribeInstances
			DescribeInstancesResult result = client.describeInstances();

			// Traverse through the reservations
			List<Reservation> reservations = result.getReservations();
			for (Reservation reservation: reservations)
			{
				// Print out the reservation id
				String reservation_id = reservation.getReservationId();
				System.out.println("Reservation: " + reservation_id);
				// Traverse through the instances in a reservation
				List<Instance> instances = reservation.getInstances();
				for (Instance instance: instances)
				{
					// Print out some information about the instance
					String id = instance.getInstanceId();
					String state = instance.getState().getName();
					System.out.println("\t" + id + "\t" + state);
				}
			}

	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	/**
	 *
	 * This demo does the following things:
	 * (1) Launch an EC2 instance in the default VPC in us-east-1 region
	 * (2) list all the EC2 instances in the us-east-1 region
	 * (3) wait for the EC2 instance launched in step (1) to become available, then terminate it.
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			// Create an instance of the DemoEC2 class
			DemoEC2 demo = new DemoEC2();
			
			// Launch a new EC2 instance for testing
			String instanceId = demo.launchInstance();
			// Sleep for 10 seconds
			Thread.sleep(10000);
			// List all the EC2 instances in the region
			demo.listInstances();
			// Sleep for 10 seconds
			Thread.sleep(10000);
			// Terminate the instance we just create
			if (!instanceId.equals("ERROR"))
			{
				demo.terminateInstance(instanceId);
			}
			// Sleep for 10 seconds
			Thread.sleep(10000);
			// List all the EC2 instances in the region (again)
			demo.listInstances();
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
