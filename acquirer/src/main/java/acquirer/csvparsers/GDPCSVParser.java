package acquirer.csvparsers;

import java.nio.file.Paths;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import acquirer.CollectionNames;

public class GDPCSVParser extends CSVParser {

	public GDPCSVParser(String directory, String dbName) {
		super(Paths.get(directory, "gdp.csv").toString(), dbName, CollectionNames.gdp);

		collection = db.getCollection(CollectionNames.gdp);
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
			System.out.println("WARNING: Skipping invalid gdp line: " + line);
		}
		catch (NumberFormatException e) {
			System.out.println("WARNING: Invalid year in gdp line: " + line);
		}
		
		return false;
	}
	
	private Boolean firstLineSkipped = false;
	private MongoCollection<Document> collection;
}
