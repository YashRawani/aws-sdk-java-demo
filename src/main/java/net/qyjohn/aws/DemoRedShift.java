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
import com.amazonaws.services.redshift.*;
import com.amazonaws.services.redshift.model.*;

public class DemoRedShift
{
	public AmazonRedshiftClient client;
	final static Logger logger = Logger.getLogger(DemoRedShift.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoRedShift()
	{
		// Create the AmazonRedshiftClient
		client = new AmazonRedshiftClient();
		// Set the region to ap-southeast-2
		client.setRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method launches an RedShift cluster. You will need to modify the parameters in this 
	 * method to make it work.
	 *
	 */

	public String launchCluster()
	{
		System.out.println("\n\nLAUNCH INSTANCE\n\n");

		try
		{

			return "OK";
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
	 * This method terminates an RedShift cluster. The parameters include
	 * @param identifier
	 *
	 */

	public void terminateCluster(String identifier)
	{
		System.out.println("\n\nTERMINATE INSTANCE\n\n");
		try
		{

		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 *
	 * This method lists all the RedShift clusters in a region.
	 *
	 */

	public void listClusters()
	{
		System.out.println("\n\nLIST INSTANCE\n\n");
        	try 
		{

	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	/**
	 *
	 * This method connects to a RedShift cluster over JDBC to do some work.
	 * This is an infinite loop and you can use CTRL C to break the execution.
	 *
	 */

	public void runJdbcTests()
	{
		System.out.println("\n\nJDBC TESTS\n\n");
		try 
		{
			// Getting database properties from db.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("redshift.properties");
			prop.load(input);
			String db_hostname = prop.getProperty("db_hostname");
			String db_username = prop.getProperty("db_username");
			String db_password = prop.getProperty("db_password");
			String db_database = prop.getProperty("db_database");

			// Load the MySQL JDBC driver
//			Class.forName("org.postgresql.Driver");
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			String jdbc_url = "jdbc:redshift://" + db_hostname + ":5439/" + db_database;

			try
			{
				// Create a connection using the JDBC driver
				Connection conn = DriverManager.getConnection(jdbc_url, db_username, db_password);

				PreparedStatement preparedStatement = conn.prepareStatement("UPDATE jdbc_test SET id=17008");
				int result = preparedStatement.executeUpdate();
				System.out.println("RETURN: " + result);

				// Do some SELECT
				Statement statement = conn.createStatement();
				String sql = "SELECT COUNT(*) as count FROM jdbc_test";
				ResultSet resultSet = statement.executeQuery(sql);
				if (resultSet.next())
				{
					long count = resultSet.getLong("count");
					System.out.println("Total Records: " + count);
				}

				// Close the connection
				conn.close();
			} catch (Exception e1)
			{
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
		} catch (Exception e0)
		{
			System.out.println(e0.getMessage());
			e0.printStackTrace();
		}
	}


	/**
	 *
	 * This demo does the following things:
	 * (1) Launch a RedShift cluster in the default VPC in us-east-1 region
	 * (2) list all RedShift clusters in the region
	 * (3) terminate the RedShift cluster we launched.
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			// Create an instance of the DemoRedShift class
			DemoRedShift demo = new DemoRedShift();
			
			String action = args[0];
			if (action.equals("launch"))
			{
				demo.launchCluster();
			}
			else if (action.equals("list"))
			{
				demo.listClusters();
			}
			else if (action.equals("terminate"))
			{
				demo.terminateCluster("Sydney");	// The argument is the RDS instance identifier
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
