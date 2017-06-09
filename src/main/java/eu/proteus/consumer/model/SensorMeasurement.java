package eu.proteus.consumer.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class SensorMeasurement extends Measurement {

	public SensorMeasurement() {
		this.type = this.getClass() == SensorMeasurement2D.class ? (byte) 0x01 : (byte) 0x00;
	}

	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void setCoilId(int coilId) {
		this.coilId = coilId;
	}

	public void setVarName(int varName) {
		this.varName = varName;
	}

	public void setX(double x) {
		this.x = x;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + coilId;
		result = prime * result + type;
		long temp;
		temp = Double.doubleToLongBits(value);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + varName;
		temp = Double.doubleToLongBits(x);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SensorMeasurement other = (SensorMeasurement) obj;
		if (coilId != other.coilId)
			return false;
		if (type != other.type)
			return false;
		if (Double.doubleToLongBits(value) != Double.doubleToLongBits(other.value))
			return false;
		if (varName != other.varName)
			return false;
		if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Row [coilId=" + coilId + ", varName=" + varName + ", value=" + value + ", type=" + type + ", x=" + x
				+ "]";
	}

}
