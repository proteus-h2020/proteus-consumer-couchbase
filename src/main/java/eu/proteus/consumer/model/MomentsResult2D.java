package eu.proteus.consumer.model;

public class MomentsResult2D extends MomentsResult {

	private double x;
	private double y;

	public MomentsResult2D(int coilId, int varId, double mean, double variance, double counter, double x, double y) {
		this.coilId = coilId;
		this.varId = varId;
		this.mean = mean;
		this.variance = variance;
		this.counter = counter;
		this.x = x;
		this.y = y;
	}

	public MomentsResult2D() {

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		long temp;
		temp = Double.doubleToLongBits(x);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(y);
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
		MomentsResult2D other = (MomentsResult2D) obj;
		if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
			return false;
		if (Double.doubleToLongBits(y) != Double.doubleToLongBits(other.y))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MomentsResult2D [x=" + x + ", y=" + y + "]";
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

}