package acquirer.csvparsers;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import acquirer.CollectionNames;

public class GDPCSVParser extends CSVParser {

	public GDPCSVParser(String filename, String dbName) {
		super(filename, dbName, CollectionNames.gdp);

		collection = db.getCollection(CollectionNames.gdp);
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
			System.out.println("WARNING: Skipping invalid gdp line: " + line);
		}
		catch (NumberFormatException e) {
			System.out.println("WARNING: Invalid year in gdp line: " + line);
		}
	}
	
	private Boolean firstLineSkipped = false;
	private MongoCollection<Document> collection;
}
