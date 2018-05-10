package com.bigdata.mapReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class MongoMapper extends Mapper<Object, BSONObject, Text, Text> {
	
	@Override
	public void map(Object key, BSONObject value, Context context) {
		System.out.println("mapper");
	}
}
