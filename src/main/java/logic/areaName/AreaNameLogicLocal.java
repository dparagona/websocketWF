package logic.areaName;

import data.model.Street;

import javax.ejb.Local;
import java.util.ArrayList;

@Local
public interface AreaNameLogicLocal {

    public ArrayList<String> getAreaNameFromCorners(float upperLeftLon, float upperLeftLat, float lowerRightLon, float lowerRightLat);

}
