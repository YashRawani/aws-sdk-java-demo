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
import com.amazonaws.services.datapipeline.*;
import com.amazonaws.services.datapipeline.model.*;

public class DemoDataPipeline 
{
	public DataPipelineClient client;
	final static Logger logger = Logger.getLogger(DemoDataPipeline.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoDataPipeline()
	{
		// Create a Data Pipeline Client
		client = new DataPipelineClient();
		// Set the region to ap-southeast-2
		client.configureRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method launches an EMR cluster. You will need to modify the parameters in this 
	 * method to make it work.
	 *
	 */

	public void describePipeline(String id)
	{
		System.out.println("\n\nDESCRIBE DATA PIPELINE\n\n");

		try
		{
			DescribePipelinesRequest request = new DescribePipelinesRequest();
			ArrayList<String> ids = new ArrayList<String>();
			ids.add(id);
			request.setPipelineIds(ids);

			DescribePipelinesResult result = client.describePipelines(request);
			List<PipelineDescription> pipelines = result.getPipelineDescriptionList();
			for (PipelineDescription pipeline: pipelines)
			{	
				List<Field> fields = pipeline.getFields();
				for (Field field : fields)
				{
					if (field.getKey().equals("@healthStatus"))
					{
						System.out.println("Health Status: " + field.getStringValue());
					}
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
	 * Test method main().
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			DemoDataPipeline demo = new DemoDataPipeline();

			String action = args[0];
			if (action.equals("describe"))
			{
				String id = args[1];
				demo.describePipeline(id);
			}
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
