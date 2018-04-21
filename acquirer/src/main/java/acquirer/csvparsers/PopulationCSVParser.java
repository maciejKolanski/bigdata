package acquirer.csvparsers;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.Document;

import acquirer.CollectionNames;
import acquirer.csvparsers.CSVParser;

import com.mongodb.client.MongoCollection;

public class PopulationCSVParser extends CSVParser {
	
	public PopulationCSVParser(String directory, String dbName, AtomicBoolean changesDetected) {
		super(Paths.get(directory, "population.csv").toString(),
				dbName, CollectionNames.population, changesDetected);
		
		collection = db.getCollection(CollectionNames.population);
	}

	@Override
	protected Boolean parseLine(String[] line) {
		if (!firstLineSkipped) {
			firstLineSkipped = true;
			return false;
		}

		try {
			int year = Integer.parseInt(line[2]);
			
			if (year <= fetchedHighestProcessedYear)
				return false;
			
			if (year > highestProcessedYear)
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
		
		return false;
	}
	
	private Boolean firstLineSkipped = false;
	private MongoCollection<Document> collection;
}
