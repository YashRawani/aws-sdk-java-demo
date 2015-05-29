package net.qyjohn.aws;

import java.io.*;
import java.net.*;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class DemoEC2 
{
	AmazonS3 s3client = new AmazonS3Client();

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
		s3client.putObject(bucketName, key, inputStream, meta);
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
