package acquirer;

import java.util.List;
import java.util.Arrays;

public class CollectionNames {
	public static final String education 	= "education";
	public static final String gdp 			= "gdp";
	public static final String population 	= "population";
	public static final String metadata	 	= "metadata";
	public static final String last_year 	= "lastyear";
	
	public static List<String> All() {
		return Arrays.asList(education, gdp, population, metadata, last_year);
	}
}
