package eu.proteus.consumer.model;

public class SensorMeasurement1D extends SensorMeasurement {

	public SensorMeasurement1D() {
	}

	public SensorMeasurement1D(int coilId, double x, int variableIdentifier, double value) {
		super();
		this.coilId = coilId;
		this.x = x;
		this.varName = variableIdentifier;
		this.value = value;
	}

	public double getX() {
		return this.x;
	}

}
