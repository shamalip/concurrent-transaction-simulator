package sim.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class AppConfig {
	private static Map<String,String> configs = loadConfigs();

	public static String get(String key){
		return configs.get(key);
	}
	
	public static Map<String,String> loadConfigs(){
		Map<String,String> configs = new HashMap<String,String>();
		InputStream inputStream = AppConfig.class.getClassLoader().getResourceAsStream("config.properties");
		try {
			Properties pr = new Properties();
			pr.load(inputStream);
			Iterator<String> it = pr.stringPropertyNames().iterator();
			while (it.hasNext())
			{
				String key = it.next();
				configs.put(key,pr.getProperty(key));
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return configs;
	} 
}
