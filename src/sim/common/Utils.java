package sim.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class Utils {

	public static List<String> readFileInToList(String fileName) 
	{ 
		List<String> lines = Collections.emptyList(); 
		try
		{ 
			lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8); 
		} 

		catch (IOException e) 
		{ 
			e.printStackTrace(); 
			// LOG correctly by passing values to jms queue.
		} 
		return lines; 
	} 

}

