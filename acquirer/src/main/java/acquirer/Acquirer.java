package acquirer;

import java.util.*;

import acquirer.CollectionNames;
import acquirer.csvparsers.*;

import com.mongodb.MongoClient;
import com.mongodb.client.*;

public class Acquirer {
	
	private static Boolean hasCollection(MongoDatabase db, String collection) {
		final MongoCursor<String> it = db.listCollectionNames().iterator();
        while (it.hasNext()) {
        	if (it.next().equalsIgnoreCase(collection))
        		return true;
        }
        
        return false;
	}
	
	private static void prepareCollections(MongoDatabase db) {
		final List<String> collectionsToCreate = Arrays.asList(
				CollectionNames.education_collection,
				CollectionNames.gdp_collection,
				CollectionNames.population_collection);
		
		for (final String collectionToCreate : collectionsToCreate) {
	        if (!hasCollection(db, collectionToCreate))
	        	db.createCollection(collectionToCreate);
	    }
	}
	
	private static void runParsers(MongoDatabase db) {
		Thread populationParser = new Thread(new PopulationCSVParser("/home/cloudera/workspace/bigdata/tests/functional/same_year/population.csv", db));
		
		populationParser.start();
	}
	
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("bigdata");
			
		prepareCollections(db);
					
		runParsers(db);
		
		mongoClient.close();
	}

}
