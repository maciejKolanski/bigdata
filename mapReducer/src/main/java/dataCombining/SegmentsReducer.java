package dataCombining;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

public class SegmentsReducer extends Reducer<SegmentKeyWritable, IntWritable, BasicBSONObject, BasicBSONObject> {

	@Override
	public void reduce(SegmentKeyWritable key, Iterable<IntWritable> education, Context context) {
		BigInteger educationSum = new BigInteger("0");
		
		int counter = 0;
		for (IntWritable educationRecord : education) {
			educationSum = educationSum.add(BigInteger.valueOf(educationRecord.get()));
			counter++;
		}
		
		educationSum = educationSum.divide(BigInteger.valueOf(counter));
		
		BasicBSONObject bsonKey = new BasicBSONObject();
		bsonKey.put("year", key.year.get());
		bsonKey.put("segment", key.segment.toString());
		
		BasicBSONObject bsonValue = new BasicBSONObject();
		bsonValue.put("meanEducationPerCapita", educationSum.intValue());
		
		try {
			context.write(bsonKey, bsonValue);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
