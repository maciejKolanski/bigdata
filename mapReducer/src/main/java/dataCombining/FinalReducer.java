package dataCombining;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;


public class FinalReducer extends Reducer<Text, JoinedWritable, BasicBSONObject, BasicBSONObject> {
	@Override
	public void reduce(Text key, Iterable<JoinedWritable> valuesIters, Context context) {		
		ChangeBuffer populationChangeBuffer = new ChangeBuffer(4);
		ChangeBuffer gdpChangeBuffer = new ChangeBuffer(4);
		
		List<JoinedWritable> values  = new ArrayList<JoinedWritable>();
		for (JoinedWritable valueIter : valuesIters)
			values.add(new JoinedWritable(valueIter));
		Collections.sort(values, new Comparator<JoinedWritable>() {
			@Override
			public int compare(JoinedWritable a, JoinedWritable b) {
				return a.year.get() - b.year.get();
			}
		});
		
		int maxYear = 0;
		
		for (JoinedWritable value : values) {
			try {
				BasicBSONObject bsonKey = new BasicBSONObject();
				bsonKey.put("code", value.code.toString());
				bsonKey.put("year", value.year.get());
				
				if (maxYear > value.year.get())
					throw new Exception("This is not sorted");
				else
					maxYear = value.year.get();
				
				populationChangeBuffer.pushChange(value.populationSegment.get());
				gdpChangeBuffer.pushChange(value.gdpSegment.get());
				
				BasicBSONObject bsonValue = new BasicBSONObject();
				bsonValue.put("countryName",  value.name.toString());
				bsonValue.put("region",  value.region.toString());
				bsonValue.put("population_segment",  value.populationSegment.get());
				bsonValue.put("population_change",  populationChangeBuffer.getChanged());
				bsonValue.put("population",  value.population.get());
				bsonValue.put("gdp_segment",  value.gdpSegment.get());
				bsonValue.put("gdp_change",  gdpChangeBuffer.getChanged());
				bsonValue.put("gdp",  value.gdp.toString());
				bsonValue.put("educationPerCapita",  value.educationPerCapita.get());
				
				context.write(bsonKey, bsonValue);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
	}
	
	private class ChangeBuffer {
		private List<Integer> changes;
		private int max;
		
		public ChangeBuffer(int n) {
			changes = new ArrayList<Integer>();
			max = n;
		}
		
		public void pushChange(int change) {
			if (changes.size() == max)
				 changes.remove(0);
			
			changes.add(change);
		}
		
		public int getChanged() {
			int change = 0;
			for (int i = 0; i < changes.size() - 1; ++i)
				change =  changes.get(i+1) - changes.get(i);
			return change;
		}
	}

}
