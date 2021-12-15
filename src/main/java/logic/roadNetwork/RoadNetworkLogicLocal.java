package logic.roadNetwork;

import data.model.Street;

import javax.ejb.Local;
import java.util.ArrayList;

@Local
public interface RoadNetworkLogicLocal {
    public ArrayList<Street> getStreetFromArea(String areaname, int zoom, int decimateSkip);
    public Street getStreet(long id);

}
