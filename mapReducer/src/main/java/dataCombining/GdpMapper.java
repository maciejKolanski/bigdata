package dataCombining;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.bson.BSONObject;

public class GdpMapper extends Mapper<Object, BSONObject, Text, Text> {
	@Override
	public void map(Object key, BSONObject value, Context context) {
		System.out.println("GdpMapper");
		System.out.println(key);
		System.out.println(value);
		System.out.println(context);
	}
}
