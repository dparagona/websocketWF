import data.model.Coordinate;
import data.model.Street;
import data.neo4j.Neo4jDAOImpl;
import mil.nga.sf.geojson.*;

import java.util.ArrayList;
import java.util.Map;

public class GeoJSONTester {


    public static void main(String[] args) {
        Neo4jDAOImpl neo4jDAO = new Neo4jDAOImpl("bolt://localhost:7687", "neo4j", "password");
        neo4jDAO.openConnection();
        ArrayList<Street> streets = neo4jDAO.getStreetFromArea("Lentilly", 18);


        Street s = streets.get(0);
        ArrayList<Coordinate> coordinates = s.getGeometry();
        ArrayList<Position> positions = new ArrayList<>();
        for (Coordinate c : coordinates){
            Position p = new Position(c.getLongitude(), c.getLatitude());
            positions.add(p);
        }

        LineString geometry = new LineString(positions);
        String content = FeatureConverter.toStringValue(geometry);
        Feature feature = new Feature(geometry);
        FeatureCollection featureCollection = new FeatureCollection();
        featureCollection.addFeature(feature);
//        String featureContent = FeatureConverter.toStringValue(feature);

//        FeatureCollection featureCollection = FeatureConverter.toFeatureCollection(geometry);
        String featureCollectionContent = FeatureConverter.toStringValue(featureCollection);

        Map<String, Object> contentMap = FeatureConverter.toMap(geometry);
        System.out.println("contentMap = " + contentMap);
    }
}
