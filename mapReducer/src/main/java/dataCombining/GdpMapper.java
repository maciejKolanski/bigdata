package dataCombining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class GdpMapper extends Mapper<Object, BSONObject, LineKeyWritable, Text> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		
		if (value.containsField("code") && value.containsField("year")) {
			try {
			
				String code = (String)value.get("code");
				int year = Integer.parseInt(((String)value.get("year")));

				int gdp = Integer.parseInt(((String)value.get("value")));
				
				context.write(
						new LineKeyWritable(code, year),
						new Text("1"));
		
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				System.out.println("Parsing error: " + value);
				e.printStackTrace();
			} 
		}
	}
}
