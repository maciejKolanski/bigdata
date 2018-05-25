package dataCombining;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

public class CombinedReducer extends Reducer<LineKeyWritable, LineValueWritable, BasicBSONObject, BasicBSONObject> {
	@Override
	public void reduce(LineKeyWritable key, Iterable<LineValueWritable> values, Context context) {
		BasicBSONObject outputKey = new BasicBSONObject();
		outputKey.put("code", key.getCode());
		outputKey.put("year", key.getYear());
		
		BasicBSONObject outputValue = new BasicBSONObject();
		for (LineValueWritable value : values) {
			switch(value.getType()) {
				case LineValueWritable.Region:
					outputValue.put("region", value.getRegion());
					break;
				case LineValueWritable.GDP:
					outputValue.put("gdp", value.getGdp());
					break;
				case LineValueWritable.Population:
					outputValue.put("population", value.getPopulation());
					break;
				case LineValueWritable.Education:
					outputValue.put("education", value.getEducation());
					break;
			};
		}
		
		try {
			if (outputValue.containsField("region")
				&& outputValue.containsField("gdp")
				&& outputValue.containsField("population")
				&& outputValue.containsField("education")) {  
				
				context.write(outputKey, outputValue);
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
