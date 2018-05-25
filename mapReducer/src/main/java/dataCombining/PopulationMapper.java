package dataCombining;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class PopulationMapper extends Mapper<Object, BSONObject, LineKeyWritable, LineValueWritable> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		
		if (value.containsField("code") && value.containsField("year") && value.containsField("name")) {
			try {
			
				final String code = (String)value.get("code");
				final String name = (String)value.get("name");
				final int year = Integer.parseInt(((String)value.get("year")));
				final int population = Integer.parseInt(((String)value.get("value")));
				
				LineValueWritable outputValue = new LineValueWritable();
				outputValue.setPopulation(population, name);
				
				context.write(
						new LineKeyWritable(code, year),
						outputValue);
		
			} catch (Exception e) {
				System.out.println("Parsing error: " + value);
				e.printStackTrace();
			}
		}
	}
}
