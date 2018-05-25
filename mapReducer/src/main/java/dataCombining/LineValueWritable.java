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
	public static final int Region	 	= 1;
	public static final int GDP 		= 2;
	public static final int Population 	= 3;
	public static final int Education 	= 4;
	
	private IntWritable type;
	private Text countryName;
	private Text region;
	private IntWritable population;
	private LongWritable gdp;
	private FloatWritable education;
	
	public static LineValueWritable FromBSON(BasicBSONObject bson) {
		try {
			LineValueWritable lineValue = new LineValueWritable();
			switch(bson.getInt("type")) {
				case Region:
					lineValue.setRegion(bson.getString("region"));
					break;
				case GDP:
					lineValue.setGdp(Long.parseLong(bson.getString("gdp")));
					break;
				case Population:
					lineValue.setPopulation(bson.getInt("population"), bson.getString("countryName"));
					break;
				case Education:
					lineValue.setEducation(Float.parseFloat(bson.getString("education")));
					break;
			};
			
			return lineValue;
		} catch (Exception e) {
			System.out.println("Unable to convert bson to LineKeyWritable: " + bson.toString());
			return null;
		}
	}
	
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

	public String getRegion() {
		return region.toString();
	}

	public String getGdp() {
		return gdp.toString();
	}
	
	public int getPopulation() {
		return population.get();
	}

	public float getEducation() {
		return education.get();
	}
	
	public void setGdp(long gdp) {
		this.gdp.set(gdp);
		this.type.set(GDP);
	}

	public void setPopulation(int population, String countryName) {
		this.countryName.set(countryName);
		this.population.set(population);
		this.type.set(Population);
	}

	public void setEducation(float educationExpenses) {
		this.education.set(educationExpenses);
		this.type.set(Education);
	}
	
	public void setRegion(String region) {
		this.region.set(region);
		this.type.set(Region);
	}
		
	public BasicBSONObject toBSON() {
		BasicBSONObject bson = new BasicBSONObject();
		bson.put("type", type.get());
		bson.put("countryName", countryName.toString());
		bson.put("region", region.toString());
		bson.put("population", population.get());
		bson.put("gdp", gdp.toString());
		bson.put("education", education.toString());
		
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
