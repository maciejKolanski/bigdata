package dataCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SegmentKeyWritable implements WritableComparable<SegmentKeyWritable> {
	public IntWritable year;
	public Text segment;
	
	public SegmentKeyWritable() {
		year = new IntWritable();
		segment = new Text();
	}
	
	public SegmentKeyWritable(int year, String segment) {
		this.year = new IntWritable(year);
		this.segment = new Text(segment);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		segment.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		year.write(out);
		segment.write(out);
		
	}

	@Override
	public int compareTo(SegmentKeyWritable o) {
		if (segment.compareTo(o.segment) != 0)
			return segment.compareTo(o.segment);
		return year.compareTo(o.year);
	}

}
