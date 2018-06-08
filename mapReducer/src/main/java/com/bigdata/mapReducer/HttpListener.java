package com.bigdata.mapReducer;

import java.io.IOException;

import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

import dataCombining.BasicReducer;
import dataCombining.CombinedMapper;
import dataCombining.CombinedReducer;
import dataCombining.EducationMapper;
import dataCombining.FinalMapper;
import dataCombining.FinalReducer;
import dataCombining.GdpMapper;
import dataCombining.JoinedWritable;
import dataCombining.LineKeyWritable;
import dataCombining.LineValueWritable;
import dataCombining.MetadataMapper;
import dataCombining.PopulationMapper;
import dataCombining.SegmentKeyWritable;
import dataCombining.SegmentsMapper;
import dataCombining.SegmentsReducer;

@RestController
public class HttpListener extends Configured {

	@RequestMapping("/mapReduce")
	public Response initMapReduce() throws Exception {
		prepareMongo();

	    JobControl jobControl = new JobControl("jobChain"); 
		
	    ControlledJob gdp = createJob(GdpMapper.class, BasicReducer.class,
	    		LineKeyWritable.class, LineValueWritable.class,  "gdp", "tmpcombined");
	    ControlledJob population = createJob(PopulationMapper.class, BasicReducer.class,
	    		LineKeyWritable.class, LineValueWritable.class, "population", "tmpcombined");
	    ControlledJob education = createJob(EducationMapper.class, BasicReducer.class,
	    		LineKeyWritable.class, LineValueWritable.class, "education", "tmpcombined");
	    ControlledJob metadata = createJob(MetadataMapper.class, BasicReducer.class,
	    		LineKeyWritable.class, LineValueWritable.class, "metadata", "tmpcombined");

    	jobControl.addJob(gdp);
    	jobControl.addJob(population);
    	jobControl.addJob(education);
    	jobControl.addJob(metadata);
	    
    	ControlledJob combined = createJob(CombinedMapper.class, CombinedReducer.class,
    			LineKeyWritable.class, LineValueWritable.class, "tmpcombined", "tmpmapped");
    	combined.addDependingJob(gdp);
    	combined.addDependingJob(population);
    	combined.addDependingJob(education);
    	combined.addDependingJob(metadata);
    	    	
    	jobControl.addJob(combined);
    	
    	ControlledJob mainJob = createJob(FinalMapper.class, FinalReducer.class,
    			Text.class, JoinedWritable.class, "tmpmapped", "output");
    	mainJob.addDependingJob(combined);
    	
    	jobControl.addJob(mainJob);
    	
    	ControlledJob segments = createJob(SegmentsMapper.class, SegmentsReducer.class,
    			SegmentKeyWritable.class, IntWritable.class, "output", "segments_output");
    	segments.addDependingJob(mainJob);
    	
    	jobControl.addJob(segments);
    	
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
	    System.out.println("Map reduce finished!");
	    exportOutput();
    	
		return Response.ok().build();
	}

	private void prepareMongo() {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("bigdata");
		
		db.getCollection("tmpcombined").drop();
		db.getCollection("tmpmapped").drop();
		db.getCollection("output").drop();
		db.getCollection("segments_output").drop();
		mongoClient.close();
	}
	
	private ControlledJob createJob(
		Class<?> mapperClass,
		Class<?> reducerClass,
		Class<?> mapOutputKeyClass,
		Class<?> mapOutputValueClass,
		String inputCollectionName,
		String outputCollectionName) throws Exception {
		
		Configuration conf = new Configuration();

		conf.setClass("mongo.job.mapper", mapperClass, mapperClass);
		conf.setClass("mongo.job.reducer", reducerClass, reducerClass);
		conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/bigdata." + inputCollectionName);
		conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/bigdata." + outputCollectionName);

		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(mapperClass.getName());
		
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		
		job.setMapOutputKeyClass(mapOutputKeyClass);
		job.setMapOutputValueClass(mapOutputValueClass);

		job.setMapperClass((Class<? extends org.apache.hadoop.mapreduce.Mapper>) mapperClass);
		job.setReducerClass((Class<? extends Reducer>) reducerClass);
		
		ControlledJob controlledJob = new ControlledJob(conf);
		controlledJob.setJob(job);
		return controlledJob;
	}

	private void exportOutput() throws Exception {
		final String mediaDir = "/media/sf_bidata/";
		{
			String segmentsCommand = "mongoexport --db bigdata --collection segments_output --type=json  --out "
					+ mediaDir + "segments_output.json";
	        Process process = Runtime.getRuntime().exec(segmentsCommand);
	        int waitFor = process.waitFor();
	        System.out.println("Exported segments output with status " + waitFor);
		}
		{
			String outputCommand = "mongoexport --db bigdata --collection output --type=json  --out "
					+ mediaDir + "main_output.json";
	        Process process = Runtime.getRuntime().exec(outputCommand);
	        int waitFor = process.waitFor();
	        System.out.println("Exported main output with status " + waitFor);
		}
		{
			String outputCommand = "touch " + mediaDir + "start";
	        Runtime.getRuntime().exec(outputCommand);
		}
	}
}
