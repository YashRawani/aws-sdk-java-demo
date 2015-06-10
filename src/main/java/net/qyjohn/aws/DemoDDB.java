package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.document.*;

public class DemoDDB 
{
	public AmazonDynamoDBClient client;
	final static Logger logger = Logger.getLogger(DemoEC2.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoDDB()
	{
		// Create the AmazonEC2Client
		client = new AmazonDynamoDBClient();
		// Set the region to ap-southeast-2
		client.setRegion(Regions.AP_SOUTHEAST_2);
	}

	public void updateItem()
	{
		try
		{
			DynamoDB db = new DynamoDB(client);
			Table table = db.getTable("Forum");

			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
			expressionAttributeNames.put("#T", "Threads");
			expressionAttributeNames.put("#stocks", "stocks");
			expressionAttributeNames.put("#models", "models");
			expressionAttributeNames.put("#colors", "colors");

			Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
			Set mySet = new HashSet<String>();
			mySet.add("2222");
			mySet.add("3333");
			expressionAttributeValues.put(":val1", mySet);  
			expressionAttributeValues.put(":val2", 10);  

			table.updateItem(
				new PrimaryKey("Name", "Test Map"),
				"ADD #stocks.#models.#colors :val1",
				"#T = :val2",
				expressionAttributeNames,
				expressionAttributeValues);
				

		} catch (Exception e)
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			DemoDDB demo = new DemoDDB();
			demo.updateItem();
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
