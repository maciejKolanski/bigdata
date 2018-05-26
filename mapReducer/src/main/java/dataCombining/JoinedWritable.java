package dataCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.bson.BasicBSONObject;

public class JoinedWritable implements WritableComparable<JoinedWritable> {
	public Text code;
	public Text name;
	public Text region;
	public IntWritable year;
	public IntWritable population;
	public IntWritable populationSegment;
	public Text gdp;
	public IntWritable gdpSegment;
	public IntWritable educationPerCapita;
	
	public static JoinedWritable FromBSON(BasicBSONObject bson) {
		JoinedWritable joined = new JoinedWritable();
		joined.code.set(bson.getString("code"));
		joined.name.set(bson.getString("countryName"));
		joined.region.set(bson.getString("region"));
		joined.year.set(bson.getInt("year"));
		joined.population.set(bson.getInt("population"));
		joined.gdp.set(bson.getString("gdp"));
		
		return joined;
	}
	
	public JoinedWritable() {
		code = new Text();
		name = new Text();
		region = new Text();
		year = new IntWritable();
		population = new IntWritable();
		populationSegment = new IntWritable();
		gdp = new Text();
		gdpSegment = new IntWritable();
		educationPerCapita = new IntWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		code.readFields(in);
		name.readFields(in);
		region.readFields(in);
		year.readFields(in);
		population.readFields(in);
		populationSegment.readFields(in);
		gdp.readFields(in);
		gdpSegment.readFields(in);
		educationPerCapita.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		code.write(out);
		name.write(out);
		region.write(out);
		year.write(out);
		population.write(out);
		populationSegment.write(out);
		gdp.write(out);
		gdpSegment.write(out);
		educationPerCapita.write(out);
	}
	@Override
	public int compareTo(JoinedWritable o) {
		if (code.compareTo(o.code) != 0)
			return code.compareTo(o.code);
		if (year.compareTo(o.year) != 0)
			return o.year.compareTo(year);
		if (name.compareTo(o.name) != 0)
			return name.compareTo(o.name);
		if (region.compareTo(o.region) != 0)
			return region.compareTo(o.region);
		if (population.compareTo(o.population) != 0)
			return population.compareTo(o.population);
		if (populationSegment.compareTo(o.populationSegment) != 0)
			return populationSegment.compareTo(o.populationSegment);
		if (gdp.compareTo(o.gdp) != 0)
			return gdp.compareTo(o.gdp);
		if (gdpSegment.compareTo(o.gdpSegment) != 0)
			return gdpSegment.compareTo(o.gdpSegment);
		
		return educationPerCapita.compareTo(o.educationPerCapita);
	}
}
