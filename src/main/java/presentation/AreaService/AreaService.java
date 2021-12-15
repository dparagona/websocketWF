package presentation.AreaService;

import data.model.Coordinate;
import data.model.Street;
import logic.areaName.AreaNameLogicLocal;
import logic.roadNetwork.RoadNetworkLogicLocal;
import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.FeatureCollection;
import mil.nga.sf.geojson.LineString;
import mil.nga.sf.geojson.Position;
import presentation.MyResposeBuilder;

import javax.ejb.EJB;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Path("/areaService")
public class AreaService implements AreaServiceAPI {

    @EJB
    private AreaNameLogicLocal areaLogic;
    @EJB
    private RoadNetworkLogicLocal roadNetworkLogic;

    public Response getAreaNameFromCorners(String upperLeft, String lowerRight) {

        if (upperLeft != null && lowerRight != null) {
            //TODO: check strings
            String[] upperLeftSplitted = upperLeft.split(",");
            float upperLeftLon = Float.parseFloat(upperLeftSplitted[1].trim());
            float upperLeftLat = Float.parseFloat(upperLeftSplitted[0].trim());

            String[] lowerRightSplitted = lowerRight.split(",");
            float lowerRightLon = Float.parseFloat(lowerRightSplitted[1].trim());
            float lowerRightLat = Float.parseFloat(lowerRightSplitted[0].trim());

            return MyResposeBuilder.createResponse(Response.Status.OK, areaLogic.getAreaNameFromCorners(upperLeftLon, upperLeftLat, lowerRightLon, lowerRightLat));
        }
        return MyResposeBuilder.createResponse(Response.Status.BAD_REQUEST);
    }

    @Override
    public Response searchStreets(String areaname, int zoom, String type, int decimateSkip) {
        if (type.equals("geojson")) {
            FeatureCollection featureCollection = convertStreetsToFeatureCollection(roadNetworkLogic.getStreetFromArea(areaname, zoom, decimateSkip));
            return MyResposeBuilder.createResponse(Response.Status.OK, featureCollection);
        }
        return MyResposeBuilder.createResponse(Response.Status.OK, roadNetworkLogic.getStreetFromArea(areaname, zoom, 0));
    }

    @Override
    public Response getStreet(long linkId) {
        return MyResposeBuilder.createResponse(Response.Status.OK, roadNetworkLogic.getStreet(linkId));
    }


    public FeatureCollection convertStreetsToFeatureCollection(ArrayList<Street> streets) {
        FeatureCollection featureCollection = new FeatureCollection();
        for (Street s : streets) {
            ArrayList<Position> positions = new ArrayList<>();
            for (Coordinate c : s.getGeometry()) {
                Position p = new Position(c.getLongitude(), c.getLatitude());
                positions.add(p);
            }
//			"properties": {
//				"name": "FRMT-DALY (ROUTE 5/6)",
//						"color": "#4db848"
            Map<String, Object> properties = new HashMap<>();
            properties.put("name", s.getName());
			if(s.getFfs()>20)
				properties.put("color", "#1199dd");
			else{
				properties.put("color", "#d21f1b");
			}
//            properties.put("color", "#fc0040");

            Feature feature = new Feature(new LineString(positions));
            feature.setProperties(properties);
            featureCollection.addFeature(feature);
        }
        return featureCollection;
    }
}
