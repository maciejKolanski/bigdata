package acquirer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.opencsv.CSVReader;

public class CSVParser implements Runnable {
	CSVParser(String filename) {
		csvFilename = filename;
	}
	
	
	public void run() {	
		try {
			Reader reader = Files.newBufferedReader(Paths.get(csvFilename), StandardCharsets.UTF_8);
			CSVReader csvReader = new CSVReader(reader);
			
	        String[] nextRecord;
	        while ((nextRecord = csvReader.readNext()) != null) {
	        	if (nextRecord.length <= 3)
	        		continue;
	        	
	            System.out.println(nextRecord[0]);
	            System.out.println(nextRecord[1]);
	            System.out.println(nextRecord[2]);
	            System.out.println(nextRecord[3]);
	        }
	        
	        csvReader.close();
		}
		catch(IOException e) {
			System.out.println("ERROR! CSV parsing of file " + csvFilename);
		}
        
	}
	
//	protected abstract void parseLine();
	
	final String csvFilename;
}
