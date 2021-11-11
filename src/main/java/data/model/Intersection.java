package data.model;

import java.util.HashMap;


public class Intersection {

	private Coordinate coordinate;
	private long osmid;
	private double betweenness;
	private String areaName;
	private HashMap<Integer, Street> streets;


	/**
	 * @param coordinate  Longitude and Latitude of Intersection in Decimal Degrees
	 *                    (DD).
	 * @param osmid       OpenStreetMap Id of the intersection in the road network
	 * @param betweenness The start value of Betweenness Centrality of the
	 *                    Intersection
	 */
	public Intersection(Coordinate coordinate, long osmid, double betweenness, String areaName) {
		super();
		this.coordinate = coordinate;
		this.osmid = osmid;
		this.betweenness = betweenness;
		this.areaName = areaName;
		this.streets = null;
	}

	/**
	 * 
	 * @param coordinate  Longitude and Latitude of Intersection in Decimal Degrees
	 *                    (DD).
	 * @param osmid       OpenStreetMap Id of the intersection in the road network
	 * @param betweenness The start value of Betweenness Centrality of the
	 *                    Intersection
	 * @param streets     Streets coming out of the intersection.
	 */
	public Intersection(Coordinate coordinate, long osmid, double betweenness, String areaName,
                        HashMap<Integer, Street> streets) {
		super();
		this.coordinate = coordinate;
		this.osmid = osmid;
		this.betweenness = betweenness;
		this.areaName = areaName;
		this.streets = streets;
	}


	public Coordinate getCoordinate() {
		return coordinate;
	}

	public void setCoordinate(Coordinate coordinate) {
		this.coordinate = coordinate;
	}

	public long getOsmid() {
		return osmid;
	}

	public void setOsmid(long osmid) {
		this.osmid = osmid;
	}

	public double getBetweenness() {
		return betweenness;
	}

	public void setBetweenness(double betweenness) {
		this.betweenness = betweenness;
	}

	public HashMap<Integer, Street> getStreets() {
		return streets;
	}

	public void setStreets(HashMap<Integer, Street> streets) {
		this.streets = streets;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

	@Override
	public String toString() {
		return "Intersection{" +
				"coordinate=" + coordinate +
				", osmid=" + osmid +
				", betweenness=" + betweenness +
				", areaName='" + areaName + '\'' +
				", streets=" + streets +
				'}';
	}
}