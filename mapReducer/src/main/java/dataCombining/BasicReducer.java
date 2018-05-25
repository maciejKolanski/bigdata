package dataCombining;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

public class BasicReducer extends Reducer<LineKeyWritable, LineValueWritable, BasicBSONObject, BasicBSONObject> {
	@Override
	public void reduce(LineKeyWritable key, Iterable<LineValueWritable> values, Context context) {
		for (LineValueWritable value : values) {
			try {
				BasicBSONObject bsonKey = key.toBSON();
				bsonKey.put("type", value.getType());
				
				context.write(bsonKey, value.toBSON());
				break;
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
