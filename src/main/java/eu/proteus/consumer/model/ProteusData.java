package eu.proteus.consumer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ProteusData {

	/**
	 * Time between coils. Obtained from the PROTEUS configuration file
	 */
	public static int TIME_BETWEEN_COILS;

	/**
	 * Coil time. Obtained from the PROTEUS configuration file
	 */
	public static int COIL_TIME;

	/**
	 * Mapping between COIL_ID and its x maximum value. Obtained from the PROTEUS-maxX.json file
	 */
	private static Map<?, ?> coil_xMax = new HashMap<String, String>();

	/**
	 * Pointer to the PROTEUS properties object
	 */
	private static Properties properties;

	/**
	 * List of flatness variable names
	 */
	public static List<Integer> FLATNESS_VARNAMES = Arrays.asList(42, 28, 11);

	static {
		properties = new Properties();
		try {
			properties.load(ProteusData.class.getClassLoader().getResource("config.properties").openStream());
			TIME_BETWEEN_COILS = Integer.parseInt((String) ProteusData.get("model.timeBetweenCoils"));
			COIL_TIME = Integer.parseInt((String) ProteusData.get("model.coilTime"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Object get(String property) {
		return properties.get("com.treelogic.proteus." + property);
	}

	public static Double getXmax(int coil) {
		String coilString = String.valueOf(coil);
		Double maxX = Double.parseDouble(String.valueOf(coil_xMax.get(coilString)));
		return maxX;
	}

	public static void loadData() {
		ClassLoader classLoader = ProteusData.class.getClassLoader();
		String json = null;
		try {
			json = new String(Files.readAllBytes(Paths.get(classLoader.getResource("PROTEUS-maxX.json").toURI())));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		try {
			coil_xMax = new ObjectMapper().readValue(json, HashMap.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
