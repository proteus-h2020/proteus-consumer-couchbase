package eu.proteus.consumer.model;

public class HSMMeasurementMapper {

	private static final String SPLITTER = (String) ProteusData.get("model.hsm.splitter");

	public static HSMMeasurement map(String record) {
		String[] lineSplit = record.split(SPLITTER);
		int coilID = Integer.parseInt(lineSplit[0]);

		HSMMeasurement hsmRecord = new HSMMeasurement(coilID);

		for (String line : lineSplit) {
			hsmRecord.put(line);
		}

		return hsmRecord;
	}
}
