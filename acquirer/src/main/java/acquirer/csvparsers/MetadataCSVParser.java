package acquirer.csvparsers;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.bson.Document;

import acquirer.CollectionNames;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

public class MetadataCSVParser implements Runnable {

	public MetadataCSVParser(String directory, String dbName) {
		csvFilename = Paths.get(directory, "population.csv").toString();
		
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase(dbName);
		collection = db.getCollection(CollectionNames.metadata);
	}

	@Override
	public void run() {
		try {
			Reader reader = Files.newBufferedReader(Paths.get(csvFilename), StandardCharsets.UTF_8);
			CSVReader csvReader = new CSVReader(reader);
			
	        String[] line = csvReader.readNext();
	        if (line != null) {
	        	collection.drop();
	        }
	        
	        while ((line = csvReader.readNext()) != null) {
				Document document = new Document();
				document.append("code", line[0]);
				document.append("region", line[1]);

				collection.insertOne(document);
	        }
	        
	        csvReader.close();
		}
		catch(IOException e) {
			System.out.println("ERROR! CSV parsing of file " + csvFilename);
		}
	}

	protected MongoDatabase db;;
	private final String csvFilename;
	private MongoClient mongoClient;
	private MongoCollection<Document> collection;
}
