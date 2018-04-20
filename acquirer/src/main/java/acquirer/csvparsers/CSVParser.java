package acquirer.csvparsers;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.bson.Document;

import acquirer.CollectionNames;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

public abstract class CSVParser implements Runnable {
	public CSVParser(String filename, String dbName, String collectionNameA) {
		csvFilename = filename;
		
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase(dbName);
		
		collectionName = collectionNameA;
		
		highestProcessedYear = fetchHighestProcessedYear();
	}
	
	public void run() {	
		try {
			Reader reader = Files.newBufferedReader(Paths.get(csvFilename), StandardCharsets.UTF_8);
			CSVReader csvReader = new CSVReader(reader);
			
	        String[] line;
	        while ((line = csvReader.readNext()) != null) {
	        	parseLine(line);
	        }
	        
	        csvReader.close();
		}
		catch(IOException e) {
			System.out.println("ERROR! CSV parsing of file " + csvFilename);
		}
		
		pushHighestProcessedYear();
	}
	
	private int fetchHighestProcessedYear() {
		MongoCollection<Document> lastProcessedYears = db.getCollection(CollectionNames.last_year);
		
		Document querry = new Document();
		querry.append("collection", collectionName);
		
		FindIterable<Document> iter = lastProcessedYears.find(querry);
		
		return iter.first().getInteger("lastProcessedYear", 0);
	}
	
	private void pushHighestProcessedYear() {
		MongoCollection<Document> lastProcessedYears = db.getCollection(CollectionNames.last_year);
		
		Document querry = new Document();
		querry.append("collection", collectionName);
		
		Document yearToUpdate = new Document();
		yearToUpdate.append("collection", collectionName);
		yearToUpdate.append("lastProcessedYear", highestProcessedYear);
		
		lastProcessedYears.replaceOne(querry, yearToUpdate);
	}
	
	protected abstract void parseLine(String[] line);
	
	protected MongoDatabase db;
	protected int highestProcessedYear;
	protected final String collectionName;
	private final String csvFilename;
	private MongoClient mongoClient;
}
