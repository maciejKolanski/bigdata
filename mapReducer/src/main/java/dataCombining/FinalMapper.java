package dataCombining;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BasicBSONObject;

public class FinalMapper extends
		Mapper<BasicBSONObject, BasicBSONObject, Text, JoinedWritable> {
	@Override
	public void map(BasicBSONObject key, BasicBSONObject value, Context context) {
		try {
			if (value.getString("region").isEmpty())
				return;
			
			value.put("code", key.getString("code"));
			value.put("year", key.getInt("year"));

			JoinedWritable joined = JoinedWritable.FromBSON(value);

			joined.educationPerCapita
				.set(calculateEducationPerCapita(
					joined.gdp.toString(),
					Float.parseFloat(value.getString("education")),
					joined.population.get())); 

			joined.populationSegment
					.set(calculatePopulationSegment(joined.population.get()));
			
			joined.gdpSegment
				.set(calculateGdpSegment(joined.gdp.toString()));
			
			context.write(joined.code, joined);
		} catch (Exception e) {
			System.out.println("Parsing error: " + value);
			e.printStackTrace();
		}
	}

	private int calculatePopulationSegment(int population) {
		if (population < 100031)
			return 0;
		else if (population < 1747383)
			return 1;
		else if (population < 5287543)
			return 2;
		else if (population < 11267329)
			return 3;
		else if (population < 51924182)
			return 4;
		else
			return 5;
	}

	private int calculateGdpSegment(String gdpStr) {
		BigInteger gdp = (new BigDecimal(gdpStr)).toBigInteger();
		
		if (gdp.compareTo(new BigInteger("384780000")) < 0)
			return 0;
		else if (gdp.compareTo(new BigInteger("1304034260")) < 0)
			return 1;
		else if (gdp.compareTo(new BigInteger("4359269460")) < 0)
			return 2;
		else if (gdp.compareTo(new BigInteger("9995956000")) < 0)
			return 3;
		else if (gdp.compareTo(new BigInteger("101895344640")) < 0)
			return 4;
		else
			return 5;
	}
	
	private int calculateEducationPerCapita(String gdpStr, float educationExpenses, int aPopulation) {
		BigInteger gdp = (new BigDecimal(gdpStr)).toBigInteger();
		BigInteger population = BigInteger.valueOf(aPopulation);
		
		int gdpPerCapita = gdp.divide(population).intValue();
		gdpPerCapita = (int) (gdpPerCapita * (educationExpenses / 100));
		
		return gdpPerCapita;
	}
}
