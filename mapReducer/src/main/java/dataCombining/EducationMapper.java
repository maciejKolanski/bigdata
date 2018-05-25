package dataCombining;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class EducationMapper extends Mapper<Object, BSONObject, LineKeyWritable, LineValueWritable> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		
		if (value.containsField("code")) {
			try {
			
				final String code = (String)value.get("code");
				
				for (String inlineKey : value.keySet()) {
					try {
						final int year = Integer.parseInt(inlineKey);
						final float educationExpenses = Float.parseFloat((String)value.get(inlineKey));
						
						LineValueWritable outputValue = new LineValueWritable();
						outputValue.setEducation(educationExpenses);
					
						context.write(
								new LineKeyWritable(code, year),
								outputValue);
				
					} catch (NumberFormatException e) {
						continue;
					}
				}
			} catch (Exception e) {
				System.out.println("Parsing error: " + value);
				e.printStackTrace();
			}
		}
	}
}
