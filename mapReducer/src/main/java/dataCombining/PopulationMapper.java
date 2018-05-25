package dataCombining;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class PopulationMapper extends Mapper<Object, BSONObject, LineKeyWritable, LineValueWritable> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		
		if (value.containsField("code") && value.containsField("year")) {
			try {
			
				String code = (String)value.get("code");
				int year = Integer.parseInt(((String)value.get("year")));
				int population = Integer.parseInt(((String)value.get("value")));
				
				LineValueWritable outputValue = new LineValueWritable();
				outputValue.setPopulation(population);
				
				context.write(
						new LineKeyWritable(code, year),
						outputValue);
		
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				System.out.println("Parsing error: " + value);
				e.printStackTrace();
			}
		}
	}
}
