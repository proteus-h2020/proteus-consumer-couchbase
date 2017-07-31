package eu.proteus.consumer.model;

import java.io.IOException;
import java.util.Properties;

public class ProteusData {

	/**
	 * Pointer to the PROTEUS properties object
	 */
	private static Properties properties;

	static {
		properties = new Properties();
		try {
			properties.load(ProteusData.class.getClassLoader().getResource("config.properties").openStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Object get(String property) {
		return properties.get("com.treelogic.proteus." + property);
	}

}
