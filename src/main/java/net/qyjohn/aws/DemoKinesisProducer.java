package net.qyjohn.aws;

import java.io.*;
import java.nio.*;
import java.net.*;
import java.util.*;
import java.text.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;

public class DemoKinesisProducer 
{
	public AmazonKinesisClient client;
	final static Logger logger = Logger.getLogger(DemoKinesisProducer.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoKinesisProducer()
	{
		client = new AmazonKinesisClient();
		client.configureRegion(Regions.US_EAST_1);
	}

	/**
	 *
	 * This method launches a producer that keeps on sending records to the stream
	 *
	 */

	public void produceRecord(String stream)
	{
		System.out.println("\n\nPRODUCE RECORD\n\n");

		while(true)
		{
			try
			{
				PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
				putRecordsRequest.setStreamName(stream);
				List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>(); 
				for (int i = 0; i < 100; i++) 
				{
					PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
					putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
					putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
					putRecordsRequestEntryList.add(putRecordsRequestEntry); 
				}

				putRecordsRequest.setRecords(putRecordsRequestEntryList);
				PutRecordsResult putRecordsResult  = client.putRecords(putRecordsRequest);
				System.out.println("Put Result" + putRecordsResult);

				Thread.sleep(10000);	// Sleep for 100 seconds
			} catch (Exception e)
			{
				// Simple exception handling by printing out error message and stack trace
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
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
			String stream = args[0];
			DemoKinesisProducer demo = new DemoKinesisProducer();
			demo.produceRecord(stream);
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
