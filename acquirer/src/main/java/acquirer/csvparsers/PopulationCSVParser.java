package acquirer.csvparsers;

import acquirer.CollectionNames;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.BasicDBList;

public class PopulationCSVParser extends CSVParser {

	public PopulationCSVParser(String filename, MongoDatabase mongoDb) {
		super(filename, mongoDb);
		
		collection = db.getCollection(CollectionNames.population_collection);
	}

	@Override
	protected void parseLine(String[] line) {
		if (!firstLineSkipped) {
			firstLineSkipped = true;
			return;
		}
		
		BasicDBList dbList = new BasicDBList();
		dbList.add(line);
	}

	private Boolean firstLineSkipped = false;
	private MongoCollection collection;
}
