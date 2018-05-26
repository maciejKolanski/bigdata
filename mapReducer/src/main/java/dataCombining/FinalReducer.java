package dataCombining;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.google.common.collect.Lists;

public class FinalReducer extends Reducer<Text, JoinedWritable, BasicBSONObject, BasicBSONObject> {
	@Override
	public void reduce(Text key, Iterable<JoinedWritable> values, Context context) {		
		for (JoinedWritable value : values) {
			try {
				BasicBSONObject bsonKey = new BasicBSONObject();
				bsonKey.put("code", value.code.toString());
				bsonKey.put("year", value.year.get());
				

				BasicBSONObject bsonValue = new BasicBSONObject();
				bsonValue.put("countryName",  value.name.toString());
				bsonValue.put("region",  value.region.toString());
				bsonValue.put("population_segment",  value.populationSegment.get());
				bsonValue.put("population",  value.population.get());
				bsonValue.put("gdpSegment",  value.gdpSegment.get());
				bsonValue.put("gdp",  value.gdp.toString());
				bsonValue.put("educationPerCapita",  value.educationPerCapita.get());
				
				context.write(bsonKey, bsonValue);
				break;
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
