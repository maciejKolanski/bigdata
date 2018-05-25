package dataCombining;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class MetadataMapper extends Mapper<Object, BSONObject, LineKeyWritable, LineValueWritable> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		
		if (value.containsField("code") && value.containsField("region")) {
			try {
	
				final String code = (String)value.get("code");
				final String region = (String)value.get("region");
				
				LineValueWritable outputValue = new LineValueWritable();
				outputValue.setRegion(region);
			
				for (int i = 1950; i < 2020; ++i)
					context.write(
							new LineKeyWritable(code, i),
							outputValue);
				
			} catch (Exception e) {
				System.out.println("Parsing error: " + value);
				e.printStackTrace();
			}
		}
	}
}
