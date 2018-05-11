package com.bigdata.mapReducer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.ws.rs.core.Response;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

import dataCombining.EducationMapper;
import dataCombining.GdpMapper;
import dataCombining.PopulationMapper;

@RestController
public class HttpListener extends Configured {

	@RequestMapping("/mapReducer")
	public Response initMapReduce() throws Exception {
		boolean dataCombineSuccess = startDataCombineJob();
		boolean mainJobSuccess = startMainJob();
		return dataCombineSuccess && mainJobSuccess ? Response.ok().build()
				: Response.serverError().build();
	}

	private boolean startDataCombineJob() throws Exception {
		/*
		 * Configuration conf = new Configuration();
		 * 
		 * conf.setClass("mongo.job.mapper", MongoMapper.class,
		 * MongoMapper.class);
		 */
		// conf.setClass("mongo.job.reducer", reducerClass, Reducer.class);
		//
		// conf.setClass("mongo.job.mapper.output.key", IntWritable.class,
		// Object.class);
		// conf.setClass("mongo.job.mapper.output.value", DoubleWritable.class,
		// Object.class);
		//
		// conf.setClass("mongo.job.output.key", NullWritable.class,
		// Object.class);
		// conf.setClass("mongo.job.output.value", outputValueClass,
		// Object.class);

		/*
		 * conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/mydb.gdp");
		 * conf.set("mongo.output.uri",
		 * "mongodb://127.0.0.1:27017/mydb.output");
		 * 
		 * Job job = Job.getInstance(conf);
		 * job.setJarByClass(HttpListener.class);
		 * job.setJobName(this.getClass().getName());
		 * job.setInputFormatClass(MongoInputFormat.class);
		 * job.setOutputFormatClass(MongoOutputFormat.class);
		 * 
		 * job.setMapperClass(MongoMapper.class);
		 * 
		 * return job.waitForCompletion(true);
		 */
		final CountDownLatch latch = new CountDownLatch(3);
		new Thread() {
			@Override
			public void run() {
				try {
					startGdpMappingJob();
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
		new Thread() {
			@Override
			public void run() {
				try {
					startEducationExcepnsesMapinngJob();
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
		new Thread() {
			@Override
			public void run() {
				try {
					startPopulationMappingJob();
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
		new Thread() {
			@Override
			public void run() {
				try {
					latch.await();
					startMainJob();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();

		return true;
	}

	private void startGdpMappingJob() throws Exception {
		Configuration conf = new Configuration();

		conf.setClass("mongo.job.mapper", GdpMapper.class, GdpMapper.class);
		conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/mydb.gdp");
		conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/mydb.output");

		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(this.getClass().getName());
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(HttpListener.class);
		job.setJobName("gdpMappingJob");

		job.setMapperClass(GdpMapper.class);
		job.waitForCompletion(true);
	}

	private void startPopulationMappingJob() throws Exception {
		Configuration conf = new Configuration();

		conf.setClass("mongo.job.mapper", PopulationMapper.class,
				PopulationMapper.class);
		conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/mydb.population");
		conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/mydb.output");

		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(this.getClass().getName());
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(HttpListener.class);
		job.setJobName("populationMappingJob");

		job.setMapperClass(PopulationMapper.class);
		job.waitForCompletion(true);
	}

	private void startEducationExcepnsesMapinngJob() throws Exception {
		Configuration conf = new Configuration();

		conf.setClass("mongo.job.mapper", EducationMapper.class,
				EducationMapper.class);
		conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/mydb.education");
		conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/mydb.output");

		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(this.getClass().getName());
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(HttpListener.class);
		job.setJobName("educationExcepnsesMappingJob");

		job.setMapperClass(EducationMapper.class);
		job.waitForCompletion(true);
	}

	private boolean startMainJob() throws Exception {
		System.out.println("Data combining finished, starting main process");
		return true;
	}
}
