package eu.proteus.consumer.model;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class MomentsResult {
	protected int coilId;
	protected int varId;
	protected double mean;
	protected double x;
	protected double y;
	protected double variance;
	protected double counter;
	protected double stdDeviation;

	private static ObjectMapper mapper = new ObjectMapper();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + coilId;
		long temp;
		temp = Double.doubleToLongBits(counter);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(mean);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(stdDeviation);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + varId;
		temp = Double.doubleToLongBits(variance);
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
		MomentsResult other = (MomentsResult) obj;
		if (coilId != other.coilId)
			return false;
		if (Double.doubleToLongBits(counter) != Double.doubleToLongBits(other.counter))
			return false;
		if (Double.doubleToLongBits(mean) != Double.doubleToLongBits(other.mean))
			return false;
		if (Double.doubleToLongBits(stdDeviation) != Double.doubleToLongBits(other.stdDeviation))
			return false;
		if (varId != other.varId)
			return false;
		if (Double.doubleToLongBits(variance) != Double.doubleToLongBits(other.variance))
			return false;
		return true;
	}

	public double getStdDeviation() {
		return stdDeviation;
	}

	public void setStdDeviation(double stdDeviation) {
		this.stdDeviation = stdDeviation;
	}

	public int getCoilId() {
		return coilId;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public void setCoilId(int coilId) {
		this.coilId = coilId;
	}

	public int getVarId() {
		return varId;
	}

	public void setVarId(int varId) {
		this.varId = varId;
	}

	public double getMean() {
		return mean;
	}

	public void setMean(double mean) {
		this.mean = mean;
	}

	public double getVariance() {
		return variance;
	}

	public void setVariance(double variance) {
		this.variance = variance;
	}

	public double getCounter() {
		return counter;
	}

	public void setCounter(double counter) {
		this.counter = counter;
	}

	public String toJson() {
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String toJson(List<MomentsResult> moments) {
		try {
			return mapper.writeValueAsString(moments);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

}