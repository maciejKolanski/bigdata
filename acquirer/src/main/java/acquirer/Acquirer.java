package acquirer;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import acquirer.csvparsers.PopulationCSVParser;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

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
		for (final String collectionToCreate : CollectionNames.All()) {
	        if (!hasCollection(db, collectionToCreate))
	        	db.createCollection(collectionToCreate);
	    }
		
		MongoCollection<Document> last_years = db.getCollection(CollectionNames.last_year);
		if (last_years.count() == 0) {
			List<String> collections = Arrays.asList(
					CollectionNames.gdp,
					CollectionNames.education,
					CollectionNames.population);
			
			for (final String collectionName : collections) {	
				Document doc = new Document();
				doc.append("collection", collectionName);
				doc.append("lastProcessedYear", 0);
				
				last_years.insertOne(doc);
			}
		}
	}
	
	private static void runParsers(MongoDatabase db) {
		Thread populationParser = new Thread(
				new PopulationCSVParser("/home/cloudera/workspace/bigdata/tests/functional/same_year/population.csv", "bigdata"));
		
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
