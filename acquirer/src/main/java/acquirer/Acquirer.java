package acquirer;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.Document;

import acquirer.csvparsers.EducationCSVParser;
import acquirer.csvparsers.GDPCSVParser;
import acquirer.csvparsers.MetadataCSVParser;
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
	
	private static void runParsers(String inputDir, String dbName) {
		AtomicBoolean changesDetected = new AtomicBoolean();
		
		Thread populationParser = new Thread(new PopulationCSVParser(inputDir, dbName, changesDetected));
		Thread gdpParser = new Thread(new GDPCSVParser(inputDir, dbName, changesDetected));
		Thread educationParser = new Thread(new EducationCSVParser(inputDir, dbName, changesDetected));
		Thread metadataParser = new Thread(new MetadataCSVParser(inputDir, dbName, changesDetected));
		
		populationParser.start();
		gdpParser.start();
		educationParser.start();
		metadataParser.start();
		
		try {
			populationParser.join();
			System.out.println("INFO: Population CSV parser finished");
			gdpParser.join();
			System.out.println("INFO: GDP CSV parser finished");
			educationParser.join();
			System.out.println("INFO: Education CSV parser finished");
			metadataParser.join();
			System.out.println("INFO: Metadata CSV parser finished");
			
			if (changesDetected.get()) {
				System.out.println("CALLING MapReduce implementation!");
				try{
					initMapReduce();
				}
				catch(Exception e) {
					System.out.println("ERROR: Couldn't invoke mapReduce");
				}
			}
			else{
				System.out.println("No changes detected!");
			}
		}
		catch(InterruptedException e) {
			System.out.println("ERROR Threads join interrupted!");
		}
		
	}

	public static void watchdog(String inputDirPath, String dbName) {	
		while(true) {
			File trigger = new File(Paths.get(inputDirPath, "start").toString());
			
			if (trigger.exists()) {
				runParsers(inputDirPath, dbName);
				
				trigger.delete();
			}
			
			try {
				Thread.sleep(1500);
			}
			catch (InterruptedException e) {
			System.out.println("ERROR, watchdog has sleep disorders");
			}
		}
		
	}
	
	private static void initMapReduce() throws Exception {
		URL url = new URL("http://localhost:8080/mapReduce");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ conn.getResponseCode());
		}
	}
	
	public static void main(String[] args) {
		Options options = new Options();

        Option dbName = new Option("d", "dbname", true, "MongoDB database name");
        dbName.setRequired(true);
        options.addOption(dbName);

        Option directory = new Option("i", "inputdirectory", true, "Directory with input files");
        directory.setRequired(true);
        options.addOption(directory);
        
        Option clean = new Option("drop_once", "Drop MongoDB databes at start");
        options.addOption(clean);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            return;
        }
		
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase(cmd.getOptionValue("dbname"));
		if (cmd.hasOption("drop_once"))
			db.drop();
		
		prepareCollections(db);
		mongoClient.close();

		watchdog(cmd.getOptionValue("inputdirectory"), cmd.getOptionValue("dbname"));
	}
}
