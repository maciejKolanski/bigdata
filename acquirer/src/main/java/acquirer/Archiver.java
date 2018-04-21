package acquirer;

import java.io.File;
import java.nio.file.Paths;

public class Archiver {

	public static void archive(String filename) {
		File file = new File(filename);
		
		String newName = Paths.get(file.getParent(), ".__" + file.getName()).toString();
		
		file.renameTo(new File(newName));
	}
}
