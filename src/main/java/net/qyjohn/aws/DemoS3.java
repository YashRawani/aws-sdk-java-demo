package net.qyjohn.aws;

import java.io.*;
import java.net.*;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class DemoS3 
{
	AmazonS3 s3client = new AmazonS3Client();

	/**
	 *
	 * This method uploads a file to S3.
	 *
	 */

	public void upload(String bucketName, String key, File file) throws Exception
	{
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
	 * This method creates a presigned URL for a specific S3 object.
	 *
	 */

	public URL getPresignedURL(String bucketName, String key, int minutes)
	{
		// Set expiration date of the presigned URL
		java.util.Date expiration = new java.util.Date();
		long msec = expiration.getTime();
		msec += 1000 * 60 * minutes;
		expiration.setTime(msec);
			     
		// Prepare to generate the presigned URL
		GeneratePresignedUrlRequest generatePresignedUrlRequest = 
		      new GeneratePresignedUrlRequest(bucketName, key);
		generatePresignedUrlRequest.setMethod(HttpMethod.GET); 
		generatePresignedUrlRequest.setExpiration(expiration);

		// Generate the presigned URL
		URL url = s3client.generatePresignedUrl(generatePresignedUrlRequest); 
		return url;
	}

	/**
	 *
	 * This demo uploads a file to a specify bucket, then returns a presigned URL for the uploaded object.
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
			File file = new File(fileName);
			String keyName = file.getName();
			DemoS3 demo = new DemoS3();
			demo.upload(bucketName, keyName, file);
			     
			// Generate the presigned URL which will expire in 5 minutes
			URL url = demo.getPresignedURL(bucketName, keyName, 5); 
			System.out.println(url);		
	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
