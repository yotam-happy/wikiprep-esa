package edu.wiki.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Singleton wrapper around simple property configuration file handler
 * file is always wikiprep-esa.config
 */
public class WikiprepESAConfiguration {
	
	// COMMON KEYS
	public final static String SERVER_NAME = "server_name";
	public final static String SCHEMA_NAME = "schema_name";
	public final static String USERNAME = "username";
	public final static String PASSWORD = "password";
	public final static String NORMALIZED_VECTOR_SIZE_LIMIT = "normalized_vector_size_limit";
	public final static String SECOND_ORDER_BONUS_VECTOR_LIMIT = "second_order_bonus_vector_limit";
	
	static WikiprepESAConfiguration wikiprepESAConfiguration; 
	Properties configuration;
	
	private WikiprepESAConfiguration() throws FileNotFoundException, IOException {
		configuration = new Properties();
		configuration.load(new FileInputStream("wikiprep-esa.config"));
	}
	
	public static synchronized WikiprepESAConfiguration getInstance() throws FileNotFoundException, IOException {
		if (wikiprepESAConfiguration == null) {
			wikiprepESAConfiguration = new WikiprepESAConfiguration();
		}
		return wikiprepESAConfiguration;
	}
	
	public String getProperty(String key) {
		return configuration.getProperty(key);
	}
	public Integer getIntProperty(String key) {
		return Integer.decode(configuration.getProperty(key));
	}
}
