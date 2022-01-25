import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import data.model.Area;
import data.Mongo.MongoConnectionManager;

import java.util.ArrayList;

public class Tester {

    public static void main(String[] args) {
    	MongoConnectionManager connectionManager = new MongoConnectionManager();
		
		ArrayList<Position> positions = new ArrayList<>();
	    positions.add(new Position(4.586122, 45.598496));
	    positions.add(new Position(4.586122, 45.600388));
	    positions.add(new Position(4.584526,45.600388));
	    positions.add(new Position(4.584526,45.598496));
	    positions.add(new Position(4.586122, 45.598496));
		
	    Polygon polygon = new Polygon(positions);
				
	    FindIterable<Area> result = connectionManager.getCollection(null, Area.class).find(Filters.geoIntersects("polygon", polygon));
	    ArrayList<String> areaNames = new ArrayList<>();
		
	    for(Area a : result) {
	    	areaNames.add(a.getNom_com());
	    	System.out.println(a);
	    }
    }
}
