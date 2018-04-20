package acquirer.csvparsers;

import org.bson.Document;

import acquirer.CollectionNames;
import acquirer.csvparsers.CSVParser;

import com.mongodb.client.MongoCollection;

public class PopulationCSVParser extends CSVParser {

	public PopulationCSVParser(String filename, String dbName) {
		super(filename, dbName);
		
		collection = db.getCollection(CollectionNames.population);
		highestProcessedYear = fetchHighestProcessedYearFor(CollectionNames.population);
	}

	@Override
	protected void parseLine(String[] line) {
		if (!firstLineSkipped) {
			firstLineSkipped = true;
			return;
		}

		try {
			int year = Integer.parseInt(line[2]);
			if (year <= highestProcessedYear)
				return;
			
			highestProcessedYear = year;
			
			Document document = new Document();
	
			document.append("name", line[0]);
			document.append("code", line[1]);
			document.append("year", line[2]);
			document.append("value", line[3]);
			
			collection.insertOne(document);
		}
		catch (IndexOutOfBoundsException e) {
			System.out.println("WARNING: Skipping invalid population line: " + line);
		}
		catch (NumberFormatException e) {
			System.out.println("WARNING: Invalid year in population line: " + line);
		}
	}
	
	protected void pushHighestProcessedYear() {
		pushHighestProcessedYearFor(CollectionNames.population, highestProcessedYear);
	}

	private Boolean firstLineSkipped = false;
	private int highestProcessedYear;
	private MongoCollection<Document> collection;
}
