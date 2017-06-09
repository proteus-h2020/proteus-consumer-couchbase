package eu.proteus.consumer.model;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HSMMeasurement extends Measurement {

	public HSMMeasurement(int coilID) {
		this.coilId = coilID;
		this.varCounter = 1;
	}

	public HSMMeasurement(int coilID, Map<String, Object> variables) {
		this.coilId = coilID;
		this.varCounter = variables.size();
		this.variables = variables;
	}

	public void put(Object value) {
		String key = String.format("V%d", varCounter++);
		this.variables.put(key, value);
	}

	public void setCoil(int coil) {
		this.coilId = coil;
	}

	public void setVariables(Map<String, Object> variables) {
		this.variables = variables;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + coilId;
		result = prime * result + varCounter;
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
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
		HSMMeasurement other = (HSMMeasurement) obj;
		if (coilId != other.coilId)
			return false;
		if (varCounter != other.varCounter)
			return false;
		if (variables == null) {
			if (other.variables != null)
				return false;
		} else if (!variables.equals(other.variables))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "HSMMeasurement [coil=" + coilId + ", variables=" + variables + ", varCounter=" + varCounter + "]";
	}

}
