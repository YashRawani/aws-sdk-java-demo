package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.rds.*;
import com.amazonaws.services.rds.model.*;

public class DemoRDS 
{
	public AmazonRDSClient client;
	final static Logger logger = Logger.getLogger(DemoRDS.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoRDS()
	{
		// Create the AmazonRDSClient
		client = new AmazonRDSClient();
		// Set the region to ap-southeast-2
		client.setRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method launches an RDS instance. You will need to modify the parameters in this 
	 * method to make it work.
	 *
	 */

	public String launchInstance()
	{
		System.out.println("\n\nLAUNCH INSTANCE\n\n");

		try
		{
			// The CreateDBInstanceRequest object
			CreateDBInstanceRequest request = new CreateDBInstanceRequest();
			request.setDBInstanceIdentifier("Sydney");	// RDS instance name
			request.setDBInstanceClass("db.t2.micro");
			request.setEngine("MySQL");		
			request.setMultiAZ(false);
			request.setMasterUsername("username");
			request.setMasterUserPassword("password");
			request.setDBName("mydb");		// database name 
			request.setStorageType("gp2");		// standard, gp2, io1
			request.setAllocatedStorage(10);	// in GB

			// VPC security groups 
			ArrayList<String> list = new ArrayList<String>();
			list.add("sg-efcc248a");			// security group, call add() again to add more than one
			request.setVpcSecurityGroupIds(list);

			// Create the RDS instance
			DBInstance instance = client.createDBInstance(request);

			// Information about the new RDS instance
			String identifier = instance.getDBInstanceIdentifier();
			String status = instance.getDBInstanceStatus();
			Endpoint endpoint = instance.getEndpoint();
			String endpoint_url = "Endpoint URL not available yet.";
			if (endpoint != null)
			{
				endpoint_url = endpoint.toString();
			}

			// Do some printing work
			System.out.println(identifier + "\t" + status);
			System.out.println(endpoint_url);

			// Return the DB instance identifier
			return identifier;
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
	 * This method terminates an RDS instance. The parameters include
	 * @param identifier
	 *
	 */

	public void terminateInstance(String identifier)
	{
		System.out.println("\n\nTERMINATE INSTANCE\n\n");
		try
		{
			// The DeleteDBInstanceRequest 
			DeleteDBInstanceRequest request = new DeleteDBInstanceRequest();
			request.setDBInstanceIdentifier(identifier);
			request.setSkipFinalSnapshot(true);
			
			// Delete the RDS instance
			DBInstance instance = client.deleteDBInstance(request);

			// Information about the RDS instance being deleted
			String status = instance.getDBInstanceStatus();
			Endpoint endpoint = instance.getEndpoint();
			String endpoint_url = "Endpoint URL not available yet.";
			if (endpoint != null)
			{
				endpoint_url = endpoint.toString();
			}

			// Do some printing work
			System.out.println(identifier + "\t" + status);
			System.out.println(endpoint_url);
		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 *
	 * This method lists all the RDS instances in a region.
	 *
	 */

	public void listInstances()
	{
		System.out.println("\n\nLIST INSTANCE\n\n");
        	try 
		{
			// Describe DB instances
			DescribeDBInstancesResult result = client.describeDBInstances();
			
			// Getting a list of the RDS instances
			List<DBInstance> instances = result.getDBInstances();
			for (DBInstance instance : instances)
			{
				// Information about each RDS instance
				String identifier = instance.getDBInstanceIdentifier();
				String engine = instance.getEngine();
				String status = instance.getDBInstanceStatus();
				Endpoint endpoint = instance.getEndpoint();
				String endpoint_url = "Endpoint URL not available yet.";
				if (endpoint != null)
				{
					endpoint_url = endpoint.toString();
				}

				// Do some printing work
				System.out.println(identifier + "\t" + engine + "\t" + status);
				System.out.println("\t" + endpoint_url);
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
	 * This method connects to a RDS MySQL instance to do a large number of INSERT operations.
	 * This is an infinite loop and you can use CTRL C to break the execution.
	 *
	 */

	public void runJdbcTests()
	{
		try 
		{
			// Getting database properties from db.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("db.properties");
			prop.load(input);
			String db_hostname = prop.getProperty("db_hostname");
			String db_username = prop.getProperty("db_username");
			String db_password = prop.getProperty("db_password");
			String db_database = prop.getProperty("db_database");

			// Load the MySQL JDBC driver
			Class.forName("com.mysql.jdbc.Driver");
			String jdbc_url = "jdbc:mysql://" + db_hostname + "/" + db_database + "?user=" + db_username + "&password=" + db_password;

			// Run an infinite loop 
			Connection connect = null;
			while (true)
			{
				try
				{
					if (connect == null)
					{
						// Create a connection using the JDBC driver
						connect = DriverManager.getConnection(jdbc_url);

						// Do some INSERT

						// Do some SELECT
					}
				} catch (Exception e1)
				{
				}
			}

		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	/**
	 *
	 * This demo does the following things:
	 * (1) Launch an RDS instance in the default VPC in us-east-1 region
	 * (2) list all the RDS instances in the us-east-1 region
	 * (3) terminate the RDS instance we launched.
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			// Create an instance of the DemoRDS class
			DemoRDS demo = new DemoRDS();
			
			String action = args[0];
			if (action.equals("launch"))
			{
				demo.launchInstance();
			}
			else if (action.equals("list"))
			{
				demo.listInstances();
			}
			else if (action.equals("terminate"))
			{
				demo.terminateInstance("Sydney");	// The argument is the RDS instance identifier
			}
			else if (action.equals("jdbc"))
			{
				demo.runJdbcTests();
			}
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
