package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import org.apache.log4j.Logger;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class DemoS3 
{
	static AmazonS3 client = new AmazonS3Client();
	final static Logger logger = Logger.getLogger(DemoS3.class);

	/**
	 *
	 * This method uploads a file to S3.
	 *
	 */

	public void upload(String bucketName, String fileName) throws Exception
	{
		// Extract the key name from filename
		File file = new File(fileName);
		String key = file.getName();

		// Open a file input stream from the file object.
		FileInputStream inputStream = new FileInputStream(file);

		// Adding the following metadata will allow IE to download an S3 object correctly.
		ObjectMetadata meta = new ObjectMetadata();
		meta.addUserMetadata("Content-Type", "application/octet-stream");
		meta.addUserMetadata("Content-Disposition", "attachment");

		// Do the upload now.
		client.putObject(bucketName, key, inputStream, meta);
	}

	/**
	 *
	 * This demo uploads a file to a specify bucket.
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			// command line argument for the bucket name and the file name
			String bucketName = args[0];	
			String fileName   = args[1];	
				
			// Upload to S3
			DemoS3 demo = new DemoS3();
			demo.upload(bucketName, fileName);
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
