package sim.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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
		} 
		return lines; 
	}

	public static boolean isOnTime(String st, long time) throws ParseException {
		if(null == st || st.isEmpty())
			return false;
		Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(st.split("\\|")[0]);
		return (d.getTime() - time) <= 0;		
	} 

}

