package dataCombining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BasicBSONObject;

public class SegmentsMapper extends
Mapper<BasicBSONObject, BasicBSONObject, SegmentKeyWritable, IntWritable> {
	@Override
	public void map(BasicBSONObject key, BasicBSONObject value, Context context) {
		try {
			IntWritable educationPerCapita = new IntWritable(value.getInt("educationPerCapita"));
			
			context.write(
				new SegmentKeyWritable(
					key.getInt("year"),
					"overall"),
				educationPerCapita);
			
			context.write(
					new SegmentKeyWritable(
						key.getInt("year"),
						"region " + value.getString("region")),
					educationPerCapita);

			context.write(
					new SegmentKeyWritable(
						key.getInt("year"),
						"gdp " + value.getString("gdp_segment")),
					educationPerCapita);	

			context.write(
					new SegmentKeyWritable(
						key.getInt("year"),
						"population " + value.getString("population_segment")),
					educationPerCapita);			
		} catch (Exception e) {
			System.out.println("Parsing error: " + value);
			e.printStackTrace();
		}
	}
}
