package logic.areaName;

import java.util.ArrayList;

import javax.ejb.Stateless;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;

import data.model.Area;
import data.Mongo.MongoConnectionManager;

@Stateless
public class AreaNameLogic implements AreaNameLogicLocal {
    MongoConnectionManager connectionManager;

    public AreaNameLogic() {
        connectionManager = new MongoConnectionManager();
    }

    @Override
    public ArrayList<String> getAreaNameFromCorners(float upperLeftLon, float upperLeftLat, float lowerRightLon, float lowerRightLat) {


        ArrayList<Position> positions = new ArrayList<>();
        positions.add(new Position(upperLeftLon, upperLeftLat));
        positions.add(new Position(lowerRightLon, upperLeftLat));
        positions.add(new Position(lowerRightLon, lowerRightLat));
        positions.add(new Position(upperLeftLon, lowerRightLat));
        positions.add(new Position(upperLeftLon, upperLeftLat));

        Polygon polygon = new Polygon(positions);

        FindIterable<Area> result = connectionManager.getCollection(null, Area.class).find(Filters.geoIntersects("polygon", polygon));
        ArrayList<String> areaNames = new ArrayList<>();

        for (Area a : result)
            areaNames.add(a.getNom_com()+"-Northbound");//l'ho modificato io, prima non c'era la concatenazione

        return areaNames;
    }
}
