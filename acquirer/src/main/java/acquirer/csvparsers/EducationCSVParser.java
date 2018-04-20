package acquirer.csvparsers;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import acquirer.CollectionNames;

public class EducationCSVParser extends CSVParser {

	public EducationCSVParser(String directory, String dbName) {
		super(Paths.get(directory, "education.csv").toString(), dbName, CollectionNames.education);

		collection = db.getCollection(CollectionNames.education);
		years = new ArrayList<Integer>();
	}

	@Override
	protected Boolean parseLine(String[] line) {
		if (headerLines > 1 ) {
			headerLines -= 1;
			return false;
		}
		else if (headerLines == 1) {
			headerLines -= 1;
			for (int i = 4; i < line.length; ++i){
				years.add(Integer.parseInt(line[i]));
			}
			if (years.size() == 0)
				return true;
			
			int highestYearInFile = years.get(years.size() - 1);
			if (highestYearInFile > fetchedHighestProcessedYear) {
				highestProcessedYear = highestYearInFile;
				return false;
			}
			
			return true;
		}

		try {
			Document document = new Document();
			document.append("name", line[0]);
			document.append("code", line[1]);
			
			for (int i = 0; i < years.size(); ++i) {
				document.append(years.get(i).toString(), line[4 + i]);
			}
			collection.insertOne(document);
		}
		catch (IndexOutOfBoundsException e) {
			System.out.println("WARNING: Skipping invalid education line: " + line);
		}
		
		return false;
	}
	
	private List<Integer> years;
	private int headerLines = 5;
	private MongoCollection<Document> collection;
}
