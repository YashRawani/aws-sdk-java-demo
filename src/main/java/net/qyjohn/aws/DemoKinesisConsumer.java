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

public class DemoKinesisConsumer
{
	public AmazonKinesisClient client;
	final static Logger logger = Logger.getLogger(DemoKinesisConsumer.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoKinesisConsumer()
	{
		client = new AmazonKinesisClient();
		client.configureRegion(Regions.US_EAST_1);
	}


	public void listShard(String stream)
	{
		System.out.println("\n\nLIST SHARDS\n\n");

		try
		{
			String exclusiveStartShardId = null;
			do 
			{
				DescribeStreamResult result = client.describeStream(stream);
				List<Shard> shards = result.getStreamDescription().getShards();
				for (Shard shard : shards)
				{
					String shardId = shard.getShardId();
					System.out.println(shardId);
				}
			} while (exclusiveStartShardId != null);
		} catch (Exception e)
		{
		}
	}

	/**
	 *
	 * This method launches a producer that keeps on receiving records to the stream
	 *
	 */

	public void consumeRecord(String stream, String shard)
	{
		System.out.println("\n\nCONSUME RECORD\n\n");

		String shardIterator;
		try
		{
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(stream);
			getShardIteratorRequest.setShardId(shard);
			getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
			GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
			shardIterator = getShardIteratorResult.getShardIterator();

			while (true)
			{
				try
				{
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					getRecordsRequest.setShardIterator(shardIterator);
					getRecordsRequest.setLimit(25);

					GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
					List<Record> records = getRecordsResult.getRecords();
					for (Record record : records)
					{
						System.out.println(record);
					}

					shardIterator = getRecordsResult.getNextShardIterator();
				} catch (Exception e2)
				{
				}
				Thread.sleep(1000);	// Sleep for 1 second
			}
		} catch (Exception e2)
		{
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
			String action = args[0];
			String stream = args[1];
			DemoKinesisConsumer demo = new DemoKinesisConsumer();
			if (action.equals("list"))
			{
				demo.listShard(stream);
			}
			else if (action.equals("consume"))
			{
				String shard  = args[2];
				demo.consumeRecord(stream, shard);
			}
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
