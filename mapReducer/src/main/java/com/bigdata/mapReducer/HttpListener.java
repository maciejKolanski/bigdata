package com.bigdata.mapReducer;

import java.io.IOException;

import javax.ws.rs.core.Response;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HttpListener extends Configured{

	@SuppressWarnings("deprecation")
	@RequestMapping("/mapReducer")
	public Response initMapReduce() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(getConf());
		job.setJarByClass(HttpListener.class);
		job.setJobName(this.getClass().getName());

		job.setMapperClass(MongoMapper.class);
		job.setReducerClass(MongoReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? Response.ok().build() : Response.serverError().build();
	}
}
