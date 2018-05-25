package dataCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.bson.BasicBSONObject;

public class LineKeyWritable implements WritableComparable<LineKeyWritable> {
	private Text code;
	private IntWritable year;
	
	public static LineKeyWritable FromBSON(BasicBSONObject bson) {
		try {
			return new LineKeyWritable(
					bson.getString("code"),
					bson.getInt("year"));
			
		} catch (Exception e) {
			System.out.println("Unable to convert bson to LineKeyWritable: " + bson.toString());
			return null;
		}
	}
	
	public LineKeyWritable() {
		code = new Text();
		year = new IntWritable();
	}
	
	public LineKeyWritable(String code, int year) {
		this.code = new Text(code);
		this.year = new IntWritable(year);
	}
	
	public String getCode() {
		return code.toString();
	}
	
	public int getYear() {
		return year.get();
	}
	
	public BasicBSONObject toBSON() {
		BasicBSONObject bson = new BasicBSONObject();
		bson.put("code", code.toString());
		bson.put("year", year.get());
		
		return bson;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		code.readFields(in);
		year.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		code.write(out);
		year.write(out);
	}

	@Override
	public int compareTo(LineKeyWritable o) {
		final int strDiff = code.compareTo(o.code);
		if (strDiff != 0)
			return strDiff;
		else
			return year.compareTo(o.year);
	}

}
