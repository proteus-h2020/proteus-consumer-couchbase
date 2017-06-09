package eu.proteus.consumer.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SensorMeasurementMapper {

    private static final Logger logger = LoggerFactory.getLogger(SensorMeasurementMapper.class);


    public static SensorMeasurement map(String rowText) {
        String[] columns = rowText.split(",");
        columns[0] = fixCoilName(columns[0]); //BUG - Some coilId are " ". Replace it by -1
        SensorMeasurement row = null;
        switch (columns.length) {
            case 5:
                row = map2d(columns);
                break;
            case 4:
                row = map1d(columns);
                break;
            default:
                logger.warn("Unkown number of columns: " + columns.length);
                return null;
        }
        logger.debug("Current row: " + row);
        return row;
    }

    private static String fixCoilName(String coilName) {
        if (coilName.trim().equals("")) {
            coilName = "-1";
        }
        return coilName;
    }

    private static SensorMeasurement map1d(String[] columns) {
        return new SensorMeasurement1D(
                Integer.parseInt(columns[0]),
                Double.parseDouble(columns[1]),
                parseVarIdentifier(columns[2]),
                Double.parseDouble(columns[3])
        );
    }

    private static SensorMeasurement map2d(String[] columns) {
        return new SensorMeasurement2D(
                Integer.parseInt(columns[0]),
                Double.parseDouble(columns[1]),
                Double.parseDouble(columns[2]),
                parseVarIdentifier(columns[3]),
                Double.parseDouble(columns[4])
        );
    }

    private static int parseVarIdentifier(String varName){
    	return Integer.parseInt(varName.split("C")[1]);
    }

}
