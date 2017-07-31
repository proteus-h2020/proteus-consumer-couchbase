package eu.proteus.consumer.model;

public class MomentsResult1D extends MomentsResult {

	private double x;

	public MomentsResult1D(int coilId, int varId, double mean, double variance, double counter, double x) {
		this.coilId = coilId;
		this.varId = varId;
		this.mean = mean;
		this.variance = variance;
		this.counter = counter;
		this.x = x;
	}

	public MomentsResult1D() {

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		long temp;
		temp = Double.doubleToLongBits(x);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MomentsResult1D other = (MomentsResult1D) obj;
		if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
			return false;
		return true;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

}