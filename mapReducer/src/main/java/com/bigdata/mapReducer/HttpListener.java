package com.bigdata.mapReducer;

import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

import dataCombining.BasicReducer;
import dataCombining.EducationMapper;
import dataCombining.GdpMapper;
import dataCombining.LineKeyWritable;
import dataCombining.LineValueWritable;
import dataCombining.MetadataMapper;
import dataCombining.PopulationMapper;

@RestController
public class HttpListener extends Configured {

	@RequestMapping("/mapReducer")
	public Response initMapReduce() throws Exception {
		prepareMongo();

	    JobControl jobControl = new JobControl("jobChain"); 
		
	    ControlledJob gdp = createBasicJob(GdpMapper.class, "gdp");
	    ControlledJob population = createBasicJob(PopulationMapper.class, "population");
	    ControlledJob education = createBasicJob(EducationMapper.class, "education");
	    ControlledJob metadata = createBasicJob(MetadataMapper.class, "metadata");

    	jobControl.addJob(gdp);
    	jobControl.addJob(population);
    	jobControl.addJob(education);
    	jobControl.addJob(metadata);
	    
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

	    while (!jobControl.allFinished()) {
	        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
	        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
	        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
	        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
	        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
	        
	        try {
		        Thread.sleep(500);
	        } catch (Exception e) {
        		e.printStackTrace();
	        }
	    } 
    	
		return Response.ok().build();
	}

	private void prepareMongo() {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("bigdata");
		
		db.getCollection("tmpcombined").drop();
		mongoClient.close();
	}
	
	private ControlledJob createBasicJob(Class<?> mapperClass,
		String inputCollectionName) throws Exception {
		
		Configuration conf = new Configuration();

		conf.setClass("mongo.job.mapper", mapperClass, mapperClass);
		conf.setClass("mongo.job.reducer", BasicReducer.class, BasicReducer.class);
		conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/bigdata." + inputCollectionName);
		conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/bigdata.tmpcombined");

		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(mapperClass.getName());
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setMapOutputKeyClass(LineKeyWritable.class);
		job.setMapOutputValueClass(LineValueWritable.class);

		job.setJarByClass(HttpListener.class);

		job.setMapperClass((Class<? extends org.apache.hadoop.mapreduce.Mapper>) mapperClass);
		job.setReducerClass(BasicReducer.class);
		
		ControlledJob controlledJob = new ControlledJob(conf);
		controlledJob.setJob(job);
		return controlledJob;
	}
}
