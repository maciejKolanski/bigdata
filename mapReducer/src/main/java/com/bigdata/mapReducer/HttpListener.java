package com.bigdata.mapReducer;

import java.io.IOException;

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

@RestController
public class HttpListener extends Configured{

	@RequestMapping("/mapReducer")
	public Response initMapReduce() throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();

	    conf.setClass("mongo.job.mapper", MongoMapper.class, MongoMapper.class);
//	    conf.setClass("mongo.job.reducer", reducerClass, Reducer.class);
//
//	    conf.setClass("mongo.job.mapper.output.key", IntWritable.class, Object.class);
//	    conf.setClass("mongo.job.mapper.output.value", DoubleWritable.class, Object.class);
//
//	    conf.setClass("mongo.job.output.key", NullWritable.class, Object.class);
//	    conf.setClass("mongo.job.output.value", outputValueClass, Object.class);

	    conf.set("mongo.input.uri",  "mongodb://127.0.0.1:27017/bigdata.gdp");
	    conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/bigdata.output");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(HttpListener.class);
		job.setJobName(this.getClass().getName());

		job.setMapperClass(MongoMapper.class);
		job.setReducerClass(MongoReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

	    job.setInputFormatClass(MongoInputFormat.class);
	    job.setOutputFormatClass(MongoOutputFormat.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? Response.ok().build() : Response.serverError().build();
	}
}
