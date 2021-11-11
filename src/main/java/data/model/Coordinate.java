package data.model;

public class Coordinate {

	/**
	 * Longitude of Intersection in Decimal Degrees (DD).
	 */
	private double longitude;
	
	/**
	 * Latitude of Intersection in Decimal Degrees (DD).
	 */
	private double latitude;

	/**
	 * 
	 * @param longitude Longitude of Intersection in Decimal Degrees (DD).
	 * @param latitude  Latitude of Intersection in Decimal Degrees (DD).
	 */
	public Coordinate(double longitude, double latitude) {
		super();
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	@Override
	public String toString() {
		return "Coordinate [longitude=" + longitude + ", latitude=" + latitude + "]";
	}

}