package acquirer;

import java.util.*;

import com.mongodb.MongoClient;
import com.mongodb.client.*;

public class Acquirer {
	public static final String population_name = "population";
	public static final String gdp_name = "gdp";
	public static final String education_name = "education";
	
	private static Boolean hasCollection(MongoDatabase db, String collection) {
		final MongoCursor<String> it = db.listCollectionNames().iterator();
        while (it.hasNext()) {
        	if (it.next().equalsIgnoreCase(collection))
        		return true;
        }
        
        return false;
	}
	
	private static void prepareCollections(MongoDatabase db) {
		final List<String> collectionsToCreate = Arrays.asList(population_name, gdp_name, education_name);
		
		for (final String collectionToCreate : collectionsToCreate) {
	        if (!hasCollection(db, collectionToCreate))
	        	db.createCollection(collectionToCreate);
	    }
	}
	
	
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("bigdata");
			
		prepareCollections(db);
	}

}
