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
	public CSVParser(String filename, String dbName) {
		csvFilename = filename;
		
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase(dbName);
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
	
	protected int fetchHighestProcessedYearFor(String collectionName) {
		MongoCollection<Document> lastProcessedYears = db.getCollection(CollectionNames.last_year);
		
		Document querry = new Document();
		querry.append("collection", collectionName);
		
		FindIterable<Document> iter = lastProcessedYears.find(querry);
		
		return iter.first().getInteger("lastProcessedYear", 0);
	}
	
	protected void pushHighestProcessedYearFor(String collectionName, int year) {
		MongoCollection<Document> lastProcessedYears = db.getCollection(CollectionNames.last_year);
		
		Document querry = new Document();
		querry.append("collection", collectionName);
		
		Document yearToUpdate = new Document();
		yearToUpdate.append("collection", collectionName);
		yearToUpdate.append("lastProcessedYear", year);
		
		lastProcessedYears.replaceOne(querry, yearToUpdate);
	}
	
	protected abstract void parseLine(String[] line);
	protected abstract void pushHighestProcessedYear();
	
	protected MongoDatabase db;
	private final String csvFilename;
	private MongoClient mongoClient;
}
