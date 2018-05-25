package dataCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.bson.BasicBSONObject;

public class LineValueWritable implements WritableComparable<LineValueWritable> {
	public static final int NotSet 		= 0;
	public static final int Metadata 	= 1;
	public static final int GDP 		= 2;
	public static final int Population 	= 3;
	public static final int Education 	= 4;
	
	private IntWritable type;
	private Text countryName;
	private Text region;
	private IntWritable population;
	private LongWritable gdp;
	private FloatWritable education;
	
	public LineValueWritable() {
		type = new IntWritable(NotSet);
		countryName = new Text();
		region = new Text();
		population = new IntWritable();
		gdp = new LongWritable();
		education = new FloatWritable();
	}
	
	public int getType() {
		return type.get();
	}
	
	public void setMetadata(String countryName, String region) {
		this.countryName.set(countryName);
		this.region.set(region);
		this.type.set(Metadata);
	}
	
	public void setGdp(long gdp) {
		this.gdp.set(gdp);
		this.type.set(GDP);
	}
	
	public BasicBSONObject toBSON() {
		BasicBSONObject bson = new BasicBSONObject();
		bson.put("type", type.get());
		bson.put("countryName", countryName.toString());
		bson.put("region", region.toString());
		bson.put("population", population.get());
		bson.put("gdp", gdp.toString());
		bson.put("education", population.toString());
		
		return bson;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		countryName.readFields(in);
		region.readFields(in);
		population.readFields(in);
		gdp.readFields(in);
		education.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		type.write(out);
		countryName.write(out);
		region.write(out);
		population.write(out);
		gdp.write(out);
		education.write(out);
	}

	@Override
	public int compareTo(LineValueWritable o) {
		if (type.compareTo(o.type) != 0)
			return type.compareTo(o.type);
		if (countryName.compareTo(o.countryName) != 0)
			return countryName.compareTo(o.countryName);
		if (region.compareTo(o.region) != 0)
			return region.compareTo(o.region);
		if (population.compareTo(o.population) != 0)
			return population.compareTo(o.population);
		if (gdp.compareTo(o.gdp) != 0)
			return gdp.compareTo(o.gdp);
		else
			return education.compareTo(o.education);
	}

}
