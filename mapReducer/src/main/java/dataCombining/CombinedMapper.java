package dataCombining;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BasicBSONObject;

public class CombinedMapper extends Mapper<BasicBSONObject, BasicBSONObject, LineKeyWritable, LineValueWritable> {
	@Override
	public void map(BasicBSONObject key, BasicBSONObject value, Context context) {
		try {
			context.write(
					LineKeyWritable.FromBSON(key),
					LineValueWritable.FromBSON(value));
			
		} catch (Exception e) {
			System.out.println("Parsing error: " + value);
			e.printStackTrace();
		}
	}
}
