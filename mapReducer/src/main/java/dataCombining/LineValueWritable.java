package dataCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LineValueWritable implements WritableComparable<LineValueWritable> {
	private Text countryName;
	private Text region;
	private IntWritable population;
	private IntWritable gdp;
	private FloatWritable education;
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(LineValueWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
