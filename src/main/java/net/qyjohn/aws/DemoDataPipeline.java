package net.qyjohn.aws;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.log4j.Logger;
import com.google.common.collect.Lists;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.datapipeline.*;
import com.amazonaws.services.datapipeline.model.*;

public class DemoDataPipeline
{
	static DataPipelineClient client;
	final static Logger logger = Logger.getLogger(DemoDataPipeline.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public DemoDataPipeline()
	{
		// Create the DataPipelineClient
		client = new DataPipelineClient();
		// Set the region to ap-southeast-2
		client.configureRegion(Regions.AP_SOUTHEAST_2);
	}

	/**
	 *
	 * This method creates a new pipeline.
	 *
	 */

	public void createPipeline() throws Exception
	{
		System.out.println("CREATE PIPELINE.");
		
		CreatePipelineRequest request = new CreatePipelineRequest();
		request.setName("Java SDK Demo");
		String uuid = UUID.randomUUID().toString();
		request.setUniqueId(uuid);
		client.createPipeline(request);
	}


	/**
	 *
	 * This method lists all existing pipelines.
	 *
	 */

	public void listPipeline() throws Exception
	{
		System.out.println("LIST PIPELINE.");
		
		ListPipelinesResult result = client.listPipelines();
		List<PipelineIdName> list = result.getPipelineIdList();
		for (PipelineIdName pipeline : list)
		{
			System.out.println(pipeline.getId() + "\t" + pipeline.getName());
		}
	}

	/**
	 *
	 * This method puts Data Pipeline definition into a newly created pipeline.
	 * This is a simple pipeline with a single ShellCommandActivity.
	 *
	 */

	public void definePipeline(String id) throws Exception
	{
		System.out.println("Define PIPELINE.");

		// Definition of the default object
		Field defaultType = new Field().withKey("scheduleType").withStringValue("CRON");
		Field defaultScheduleType = new Field().withKey("schedule").withRefValue("RunOnceSchedule");
		Field defaultFailureAndRerunMode = new Field().withKey("failureAndRerunMode").withStringValue("CASCADE");
		Field defaultRole = new Field().withKey("role").withStringValue("DataPipelineDefaultRole");
		Field defaultResourceRole = new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole");
		Field defaultLogUri = new Field().withKey("pipelineLogUri").withStringValue("s3://331982-syd/java-dp-log");
		List<Field> defaultFieldList = Lists.newArrayList(defaultType, defaultScheduleType, defaultFailureAndRerunMode, defaultRole, defaultResourceRole, defaultLogUri);
		PipelineObject defaultObject = new PipelineObject().withName("Default").withId("Default").withFields(defaultFieldList);

		// Definition of the pipeline schedule
		Field scheduleType = new Field().withKey("type").withStringValue("Schedule");
		Field scheduleStartAt = new Field().withKey("startAt").withStringValue("FIRST_ACTIVATION_DATE_TIME");
		Field schedulePeriod = new Field().withKey("period").withStringValue("1 day");
		Field scheduleOccurrences = new Field().withKey("occurrences").withStringValue("1");
		List<Field> scheduleFieldList = Lists.newArrayList(scheduleType, scheduleStartAt, schedulePeriod, scheduleOccurrences);
		PipelineObject schedule = new PipelineObject().withName("RunOnceSchedule").withId("RunOnceSchedule").withFields(scheduleFieldList);

		// Definition of the Ec2Resource
		Field ec2Type = new Field().withKey("type").withStringValue("Ec2Resource");
		Field ec2TerminateAfter = new Field().withKey("terminateAfter").withStringValue("15 minutes");
//		Field ec2Role = new Field().withKey("role").withStringValue("DataPipelineDefaultRole");
//		Field ec2ResourceRole = new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole");
		List<Field> ec2FieldList = Lists.newArrayList(ec2Type, ec2TerminateAfter);
		PipelineObject ec2 = new PipelineObject().withName("Ec2Instance").withId("Ec2Instance").withFields(ec2FieldList);

		// Definition of the ShellCommandActivity
		// The ShellCommandActivity is a command "df -h"
		Field activityType = new Field().withKey("type").withStringValue("ShellCommandActivity");
		Field activityRunsOn = new Field().withKey("runsOn").withRefValue("Ec2Instance");
		Field activityCommand = new Field().withKey("command").withStringValue("df -h");
		Field activityStdout = new Field().withKey("stdout").withStringValue("s3://331982-syd/dp-java-demo-stdout");
		Field activityStderr = new Field().withKey("stderr").withStringValue("s3://331982-syd/dp-java-demo-stderr");
		Field activitySchedule = new Field().withKey("schedule").withRefValue("RunOnceSchedule");
		List<Field> activityFieldList = Lists.newArrayList(activityType, activityRunsOn, activityCommand, activityStdout, activityStderr, activitySchedule);
		PipelineObject activity = new PipelineObject().withName("DfCommand").withId("DfCommand").withFields(activityFieldList);

		// setPipelineObjects
		List<PipelineObject> objects = Lists.newArrayList(defaultObject, schedule, ec2, activity);

		// putPipelineDefinition
		PutPipelineDefinitionRequest request = new PutPipelineDefinitionRequest();
		request.setPipelineId(id);
		request.setPipelineObjects(objects);
		PutPipelineDefinitionResult putPipelineResult = client.putPipelineDefinition(request);

		if (putPipelineResult.isErrored()) 
		{
			logger.error("Error found in pipeline definition: ");
			putPipelineResult.getValidationErrors().stream().forEach(e -> logger.error(e));
		}

		if (putPipelineResult.getValidationWarnings().size() > 0) 
		{
			logger.warn("Warnings found in definition: ");
			putPipelineResult.getValidationWarnings().stream().forEach(e -> logger.warn(e));
		}


	}


	/**
	 *
	 * This method activates a pipeline.
	 *
	 */

	public void activatePipeline(String id) throws Exception
	{
		System.out.println("ACTIVATE PIPELINE.");	

		ActivatePipelineRequest request = new ActivatePipelineRequest();
		request.setPipelineId(id);
		client.activatePipeline(request);
	}



	/**
	 *
	 * This method deletes a pipeline.
	 *
	 */

	public void deletePipeline(String id) throws Exception
	{
		System.out.println("DELETE PIPELINE.");	

		DeletePipelineRequest request = new DeletePipelineRequest();
		request.setPipelineId(id);
		client.deletePipeline(request);
	}

	/**
	 *
	 * This demo does the following things:
	 * (1) Creates a new pipeline.
	 * (2) Lists all existing pipelines.
	 * (3) Puts pipeline definition to an existing pipelines.
	 * (4) Activates an existing pipelines.
	 * (5) Deletes an existing pipeline.
	 *
	 */

	public static void main(String[] args) throws IOException 
	{
        	try 
		{
			DemoDataPipeline demo = new DemoDataPipeline();
			String action = args[0];
			if (action.equals("create"))
			{
				demo.createPipeline();
			}
			else if (action.equals("list"))
			{
				demo.listPipeline();
			}
			else if (action.equals("define"))
			{
				demo.definePipeline(args[1]);
			}
			else if (action.equals("activate"))
			{
				demo.activatePipeline(args[1]);
			}
			else if (action.equals("delete"))
			{
				demo.deletePipeline(args[1]);
			}

	        } catch (Exception e) 
		{
			// Simple exception handling by printing out error message and stack trace
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
