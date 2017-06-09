package eu.proteus.consumer.model;

import java.util.HashMap;
import java.util.Map;

public abstract class Measurement {

	protected int coilId;
	protected int varName;
	protected double value;
	protected byte type;
	protected double x;
	protected double y;
	protected Map<String, Object> variables = new HashMap<String, Object>();
	protected int varCounter;

	public Map<String, Object> getHSMVariables() {
		return this.variables;
	}

	public int getHSMVarCounter() {
		return this.varCounter;
	}

	public int getVarName() {
		return this.varName;
	}

	public double getValue() {
		return this.value;
	}

	public double getPositionX() {
		return this.x;
	}

	public double getPositionY() {
		return this.y;
	}

	public int getCoilID() {
		return this.coilId;
	}

	public String getStringCoilID() {
		return Integer.toString(this.coilId);
	}

	public byte getType() {
		return this.type;
	}

}
