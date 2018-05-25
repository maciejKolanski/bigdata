package dataCombining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BasicReducer extends Reducer<LineKeyWritable, Text, Text, IntWritable> {
	@Override
	public void reduce(LineKeyWritable key, Iterable<Text> values, Context context) {
		System.out.println("Basicreducer");
		System.out.println(key.code);
	}
}
