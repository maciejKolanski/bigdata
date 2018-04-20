package acquirer.csvparsers;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

public abstract class CSVParser implements Runnable {
	public CSVParser(String filename, MongoDatabase mongoDb) {
		csvFilename = filename;
		db = mongoDb;
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
        
	}
	
	protected abstract void parseLine(String[] line);
	
	
	protected MongoDatabase db;
	private final String csvFilename;
}
