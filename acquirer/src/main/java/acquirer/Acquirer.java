package acquirer;

import java.util.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;

public class Acquirer {
	public static final String education_collection 	= "education";
	public static final String gdp_collection 			= "gdp";
	public static final String population_collection 	= "population";
	
	private static Boolean hasCollection(MongoDatabase db, String collection) {
		final MongoCursor<String> it = db.listCollectionNames().iterator();
        while (it.hasNext()) {
        	if (it.next().equalsIgnoreCase(collection))
        		return true;
        }
        
        return false;
	}
	
	private static void prepareCollections(MongoDatabase db) {
		final List<String> collectionsToCreate = Arrays.asList(education_collection, gdp_collection, population_collection);
		
		for (final String collectionToCreate : collectionsToCreate) {
	        if (!hasCollection(db, collectionToCreate))
	        	db.createCollection(collectionToCreate);
	    }
	}
	
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase("bigdata");
			
		prepareCollections(db);
				
		(new Thread(new CSVParser("/home/cloudera/workspace/bigdata/tests/functional/same_year/education.csv"))).start();
		
		mongoClient.close();
	}

}
