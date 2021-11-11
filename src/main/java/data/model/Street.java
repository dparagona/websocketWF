package data.model;

import java.util.ArrayList;

public class Street {

	private ArrayList<Coordinate> geometry;
	private long linkId;
	private long from, to;
	private double lenght;
	private int speedLimit;
	private String areaName;
	private String name;
	private double weight;
	private double ffs;
	private long frc;
	private long netClass;
	private String routeNumber;
	private long fow;


	/**
	 * 
	 * @param geometry       Describes Geometry of the street.
	 * @param linkId            Local id of the Street.
	 * @param from        		OpenStreetMap id of the starting intersection.
	 * @param to         		OpenStreetMap id of the destination intersection.
	 * @param lenght            Lenght in meter of the street.
	 * @param speedLimit        Speed limit of the street in km/h.
	 * @param name              Name of the Street.
	 * @param weight            Weight value of links for graph elaborations.
	 * @param ffs               Free flow speed value used for graph elaborations.
	 */
	public Street(ArrayList<Coordinate> geometry, long linkId, long from, long to, double lenght, int speedLimit, String areaName, String name, double weight, double ffs, long frc, long netClass, String routeNumber, long fow) {
		this.geometry = geometry;
		this.linkId = linkId;
		this.from = from;
		this.to = to;
		this.lenght = lenght;
		this.speedLimit = speedLimit;
		this.areaName = areaName;
		this.name = name;
		this.weight = weight;
		this.ffs = ffs;
		this.frc = frc;
		this.netClass = netClass;
		this.routeNumber = routeNumber;
		this.fow = fow;
	}

	public ArrayList<Coordinate> getGeometry() {
		return geometry;
	}

	public void setGeometry(ArrayList<Coordinate> geometry) {
		this.geometry = geometry;
	}

	public long getLinkId() {
		return linkId;
	}

	public void setLinkId(long linkId) {
		this.linkId = linkId;
	}

	public long getFrom() {
		return from;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public long getTo() {
		return to;
	}

	public void setTo(long to) {
		this.to = to;
	}

	public double getLenght() {
		return lenght;
	}

	public void setLenght(double lenght) {
		this.lenght = lenght;
	}

	public int getSpeedLimit() {
		return speedLimit;
	}

	public void setSpeedLimit(int speedLimit) {
		this.speedLimit = speedLimit;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getFfs() {
		return ffs;
	}

	public void setFfs(double ffs) {
		this.ffs = ffs;
	}

	public long getFrc() {
		return frc;
	}

	public void setFrc(long frc) {
		this.frc = frc;
	}

	public long getNetClass() {
		return netClass;
	}

	public void setNetClass(long netClass) {
		this.netClass = netClass;
	}

	public String getRouteNumber() {
		return routeNumber;
	}

	public void setRouteNumber(String routeNumber) {
		this.routeNumber = routeNumber;
	}

	public long getFow() {
		return fow;
	}

	public void setFow(long fow) {
		this.fow = fow;
	}

	@Override
	public String toString() {
		return "Street{" +
				"geometry=" + geometry +
				", linkId=" + linkId +
				", from=" + from +
				", to=" + to +
				", lenght=" + lenght +
				", speedLimit=" + speedLimit +
				", areaName='" + areaName + '\'' +
				", name='" + name + '\'' +
				", weight=" + weight +
				", ffs=" + ffs +
				", frc=" + frc +
				", netClass=" + netClass +
				", routeNumber='" + routeNumber + '\'' +
				", fow=" + fow +
				'}';
	}
}