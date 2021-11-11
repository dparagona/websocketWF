package data.neo4j;

import data.DatabaseNotConnectException;
import data.model.Coordinate;
import data.model.Intersection;
import data.model.Street;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class IntersectionFieldsNeo4j {
    public static final String LONGITUDE = "longitude";
    public static final String LATITUDE = "latitude";
    public static final String OSMID = "osmid";
    public static final String AREANAME = "areaName";
    public static final String BETWEENNESS = "betweenness";
}

class StreetFieldsNeo4j {
    public static final String LINKID = "linkId";
    public static final String COORDINATES = "geometry";
    public static final String TO = "to";
    public static final String FROM = "from";
    public static final String LENGTH = "length";
    public static final String SPEEDLIMIT = "speedLimit";
    public static final String AREANAME = "areaName";
    public static final String NAME = "name";
    public static final String WEIGHT = "weight";
    public static final String FFS = "ffs";
    public static final String FRC = "frc";
    public static final String NETCLASS = "netClass";
    public static final String ROUTENUMBER = "routeNumber";
    public static final String FOW = "fow";


}

public class Neo4jDAOImpl {

    private final String uri, user, password;
    private Driver driver;
    private static final Logger logger = LoggerFactory.getLogger(Neo4jDAOImpl.class);
    private final SessionConfig readSessionConfig;
    private final SessionConfig writeSessionConfig;


    /**
     * @param uri      is the bolt address to access neo4j database.
     * @param user     is the username to access neo4j database.
     * @param password is the password to access neo4j database.
     */
    public Neo4jDAOImpl(String uri, String user, String password) {
        //System.out.println();
        logger.info("DAOUserNeo4jImpl.DAOUserNeo4jImpl: uri = " + uri + ", user = " + user + ", password = " + password);
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.driver = null;
        this.readSessionConfig = SessionConfig.builder()
                .withDefaultAccessMode(AccessMode.READ)
                .build();
        this.writeSessionConfig = SessionConfig.builder()
                .withDefaultAccessMode(AccessMode.WRITE)
                .build();
    }

    public void openConnection() {
        logger.info("DAOUserNeo4jImpl.openConnection");
        logger.info("Opening Connection to DataBase URI[" + uri + "]");
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    public void closeConnection() {
        logger.info("DAOUserNeo4jImpl.closeConnection");
        logger.info("Closing Connection to DataBase URI[" + uri + "]");
        // Logica bloccante
        // driver.close();

        // Logica non bloccante
        driver.closeAsync();
        driver = null;
    }

    public Result databaseRead(String query) {
        logger.debug("DAOUserNeo4jImpl.databaseRead:query = " + query);
        try {
            if (driver == null)
                throw new DatabaseNotConnectException("Database Non Connesso");
            Session session = driver.session(readSessionConfig);
            return session.run(query);
        } catch (DatabaseNotConnectException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Result databaseWrite(String query) {
        logger.debug("DAOUserNeo4jImpl.databaseRead:query = " + query);
        try {
            if (driver == null)
                throw new DatabaseNotConnectException("Database Non Connesso");
            Session session = driver.session(writeSessionConfig);
            return session.run(query);
        } catch (DatabaseNotConnectException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Intersection getIntersection(long osmid) {
        logger.debug("DAOUserNeo4jImpl.getIntersection: osmid = " + osmid);
        String query = String.format("MATCH (a:Intersection {%s:%s}) RETURN properties(a)", IntersectionFieldsNeo4j.OSMID, osmid);
        Record resultRecord = databaseRead(query).single();
        Value v = resultRecord.get("properties(a)");
        return extractIntersection(v);
    }

    public Intersection getIntersectionLight(long osmid) {
        logger.debug("DAOUserNeo4jImpl.getIntersectionLight: osmid = " + osmid);

        String query = String.format("MATCH (a:Intersection {%s:%s}) RETURN properties(a)", IntersectionFieldsNeo4j.OSMID, osmid);
        Record resultRecord = databaseRead(query).single();
        try {
            Value v = resultRecord.get("properties(a)");
            return extractIntersection(v);
        } catch (NoSuchRecordException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Street getStreet(long osmidS, long osmidD) {
        logger.debug("DAOUserNeo4jImpl.getStreet: osmidS = " + osmidS + ", osmidD = " + osmidD);
        String query = String.format("MATCH (a:Intersection{%s:%s})-[r:STREET]->(b:Intersection{%s:%s}) RETURN properties(r)",
                IntersectionFieldsNeo4j.OSMID, osmidS, IntersectionFieldsNeo4j.OSMID, osmidD);

        Result result = databaseRead(query);
        Record r = result.next();

        Value v = r.get("properties(r)");

        return extractStreet(v);

    }


    public Street getStreet(long id) {
        logger.debug("DAOUserNeo4jImpl.getStreet: id = " + id);
        String query = String.format("MATCH (a:Intersection)-[r:STREET]->(b:Intersection) WHERE r.%s=%s RETURN properties(r)", StreetFieldsNeo4j.LINKID, id);
        Result result = databaseRead(query);
        Record r = result.single();
        Value v = r.get("properties(r)");

        return extractStreet(v);
    }

    public ArrayList<Street> getStreetFromArea(String areaname, int zoom) {
        int frc = zoom - 10;
        String query = String.format("MATCH ()-[s:STREET]->() WHERE s.%s=\"%s\" AND s.%s<=%s RETURN properties(s) as s", StreetFieldsNeo4j.AREANAME, areaname, StreetFieldsNeo4j.FRC, frc);
        Result result = databaseRead(query);
        ArrayList<Street> streets = new ArrayList<>();
        for (Record record : result.list()) {
//            System.out.println("record = " + record);
            Street s = extractStreet(record);
            //System.out.println(s);
            streets.add(s);
//            System.out.println("s = " + s);

        }
        return streets;
    }

    public ArrayList<Coordinate> getStreetGeometry(long osmidS, long osmidD) {
        logger.debug("DAOUserNeo4jImpl.getStreetGeometry: osmidS = " + osmidS + ", osmidD = " + osmidD);

        String query = String.format("MATCH (a:Intersection{%s:%s})-[r:STREET]->(b:Intersection{%s:%s}) RETURN r.coordinates", IntersectionFieldsNeo4j.OSMID, osmidS, IntersectionFieldsNeo4j.OSMID, osmidD);
        String sCord = databaseRead(query).list().get(0).get("r.coordinates").asString();
        return getCoordinateList(sCord);
    }

    private ArrayList<Coordinate> getCoordinateList(String sCord) {
        logger.debug("DAOUserNeo4jImpl.getCoordinateList: sCord = " + sCord);
        String[] split = sCord.split("\\|");
        String[] split2;
        ArrayList<Coordinate> coordinates = new ArrayList<>();
        Coordinate c;
        for (String s : split) {
            split2 = s.split(",");
            c = new Coordinate(Double.parseDouble(split2[1]), Double.parseDouble(split2[0]));
            coordinates.add(c);
        }
        return coordinates;
    }


    public HashMap<Integer, Street> getStreets(long osmid) {
        logger.debug("DAOUserNeo4jImpl.getStreets: osmid = " + osmid);

        String query = String.format("MATCH (a:Intersection {%s:%s})-[r:STREET]->(b:Intersection) RETURN collect(r.%s) as ids", IntersectionFieldsNeo4j.OSMID, osmid, StreetFieldsNeo4j.LINKID);
        Result result = databaseRead(query);

        HashMap<Integer, Street> strade = new HashMap<>();
        List<Object> ids = result.single().get("ids").asList();
        for (Object o : ids) {
            int id = ((Long) o).intValue();
            Street s = getStreet(id);
            strade.put(id, s);
        }
        return strade;
    }


    // SERVICE METHODS

    public ArrayList<Coordinate> shortestPathCoordinate(long osmidStart, long osmidDest) {
        logger.debug("DAOUserNeo4jImpl.shortestPathCoordinateIgnoreInterrupted: osmidStart = " + osmidStart + ", osmidDest = " + osmidDest);

        String query = String.format("MATCH (start:Intersection{%s:%s}), (end:Intersection{%s:%s}) " +
                "CALL algo.shortestPath.stream(start, end, 'weight',{direction:'OUTGOING'}) YIELD nodeId, cost " +
                "RETURN algo.asNode(nodeId).osmid as vertexKeys", IntersectionFieldsNeo4j.OSMID, osmidStart, IntersectionFieldsNeo4j.OSMID, osmidDest);
        Result result = databaseRead(query);
        ArrayList<Long> shortestPath = new ArrayList<>();
        Record r;
        long vertexKey;
        while (result.hasNext()) {
            r = result.next();
            vertexKey = r.get("vertexKeys").asLong();

            shortestPath.add(vertexKey);
        }
        ArrayList<Coordinate> coordstmp = new ArrayList<>();
        ArrayList<Coordinate> coords = new ArrayList<>();

        for (int i = 0; i < shortestPath.size() - 1; i++) {
            coordstmp = getStreetGeometry(shortestPath.get(i), shortestPath.get(i + 1));
            coords.addAll(coordstmp);
        }
        return coords;

    }

    public ArrayList<Intersection> shortestPath(long osmidStart, long osmidDest) {
        logger.debug("DAOUserNeo4jImpl.shortestPathIgnoreInterrupted: osmidStart = " + osmidStart + ", osmidDest = " + osmidDest);
        String query = String.format("MATCH (start:Intersection{%s:%s}), (end:Intersection{%s:%s}) " +
                "CALL algo.shortestPath.stream(start, end, 'weight',{direction:'OUTGOING'}) YIELD nodeId, cost " +
                "RETURN algo.asNode(nodeId) as intersections", IntersectionFieldsNeo4j.OSMID, osmidStart, IntersectionFieldsNeo4j.OSMID, osmidDest);
        Result result = databaseRead(query);
        return extractIntersectionArrayList(result);
    }

    public ArrayList<Intersection> getTopCriticalNodes(int top) {
        logger.debug("DAOUserNeo4jImpl.getTopCriticalNodes: top = " + top);
        String query = "MATCH (i:Intersection) RETURN properties(i) ORDER BY i.betweenness DESC LIMIT " + top;
        Result result = databaseRead(query);
        return convertToIntersectionArrayList(result, "i");
    }

    public ArrayList<Intersection> getThresholdCriticalNodes(double threshold) {
        logger.debug("DAOUserNeo4jImpl.getThresholdCriticalNodes: threshold = " + threshold);
        String query = "MATCH (i:Intersection) WHERE i.betweenness > " + threshold + " RETURN properties(i)";
        Result result = databaseRead(query);
        return convertToIntersectionArrayList(result, "i");
    }

    public int getLinkKey(long osmidStart, long osmidDest) {
        logger.debug("DAOUserNeo4jImpl.getLinkKey: osmidStart = " + osmidStart + ", osmidDest = " + osmidDest);
        String query = "MATCH (a:Intersection{osmid:" + osmidStart + "})-[r:STREET]->(b:Intersection{osmid:" + osmidDest
                + "}) RETURN r.id";
        Result result = databaseRead(query);

        return result.single().get("r.id").asInt();
    }

    public Intersection getNearestIntersection(Coordinate position) {
        logger.debug("DAOUserNeo4jImpl.getNearestIntersection: position = " + position);

        String query = "MATCH (t:Intersection)  " + "WITH t , distance(point({ longitude: " + position.getLongitude()
                + ", latitude: " + position.getLatitude()
                + " }), point({ longitude: t.longitude, latitude: t.latitude })) AS distance " + "order by distance "
                + "return properties(t), distance  LIMIT 1";

        Result result = databaseRead(query);
        Record r = result.single();

        double dist = r.get("distance").asDouble();
        if (dist > 10000) {
            return null; // TODO verifica
        }
        Value v = r.get("properties(t)");

        return extractIntersection(v);
    }

    private ArrayList<Intersection> convertToIntersectionArrayList(Result result, String nodeName) {
        logger.debug("DAOUserNeo4jImpl.convertToIntersectionArrayList: result = " + result + ", nodeName = " + nodeName);
        ArrayList<Intersection> intersections = new ArrayList<>();
        Record r;
        Value v;
        while (result.hasNext()) {
            r = result.next();
            v = r.get("properties(" + nodeName + ")");
            Coordinate c = new Coordinate(r.get(IntersectionFieldsNeo4j.LONGITUDE).asDouble(), r.get(IntersectionFieldsNeo4j.LATITUDE).asDouble());
            long id = r.get(IntersectionFieldsNeo4j.OSMID).asLong();
            double betweenness = r.get(IntersectionFieldsNeo4j.BETWEENNESS).asDouble();
            String areaName = r.get(IntersectionFieldsNeo4j.AREANAME).asString();
            intersections.add(new Intersection(c, id, betweenness, areaName));
        }
        return intersections;
    }

    private Intersection extractIntersection(Value r) {
        logger.debug("DAOUserNeo4jImpl.convertIntersection: r = " + r);
        try {
            Coordinate c = new Coordinate(r.get(IntersectionFieldsNeo4j.LONGITUDE).asDouble(), r.get(IntersectionFieldsNeo4j.LATITUDE).asDouble());
            long id = r.get(IntersectionFieldsNeo4j.OSMID).asLong();
            double betweenness = r.get(IntersectionFieldsNeo4j.BETWEENNESS).asDouble();
            String areaName = r.get(IntersectionFieldsNeo4j.AREANAME).asString();
            return new Intersection(c, id, betweenness, areaName);
        } catch (NoSuchRecordException e) {
            e.printStackTrace();
            return null;
        }
    }

    private ArrayList<Intersection> extractIntersectionArrayList(Result result) {
        logger.debug("DAOUserNeo4jImpl.extractIntersectionArrayList: result = " + result);

        ArrayList<Intersection> shortestPath = new ArrayList<>();
        for (Record r : result.list()) {
            Map<String, Object> mapValues = r.get("intersections").asMap();
            long osmid = (long) mapValues.get(IntersectionFieldsNeo4j.OSMID);
            double betweenness = (double) mapValues.get(IntersectionFieldsNeo4j.BETWEENNESS);
            double latitude = (double) mapValues.get(IntersectionFieldsNeo4j.LATITUDE);
            double longitude = (double) mapValues.get(IntersectionFieldsNeo4j.LONGITUDE);
            String areaName = r.get(IntersectionFieldsNeo4j.AREANAME).asString();
            Intersection i = new Intersection(new Coordinate(longitude, latitude), osmid, betweenness, areaName);
            shortestPath.add(i);
        }
        return shortestPath;
    }

    private Street extractStreet(Value v) {
        String sCord = v.get(StreetFieldsNeo4j.COORDINATES).asString();
        ArrayList<Coordinate> coordinates = getCoordinateList(sCord);
        long linkId = v.get(StreetFieldsNeo4j.LINKID).asLong();
        long from = v.get(StreetFieldsNeo4j.FROM).asLong();
        long to = v.get(StreetFieldsNeo4j.TO).asLong();
        double length = v.get(StreetFieldsNeo4j.LENGTH).asDouble();
        int speedlimit = v.get(StreetFieldsNeo4j.SPEEDLIMIT).asInt();
        String areaname = v.get(StreetFieldsNeo4j.AREANAME).asString();
        String name = v.get(StreetFieldsNeo4j.NAME).asString();
        double weight = v.get(StreetFieldsNeo4j.WEIGHT).asDouble();
        double ffs = v.get(StreetFieldsNeo4j.FFS).asDouble();
        long frc = v.get(StreetFieldsNeo4j.FRC).asLong();
        long netClass = v.get(StreetFieldsNeo4j.NETCLASS).asLong();
        String routeNumber = v.get(StreetFieldsNeo4j.ROUTENUMBER).asString();
        long fow = v.get(StreetFieldsNeo4j.FOW).asLong();

        return new Street(coordinates, linkId, from, to, length, speedlimit, areaname, name, weight, ffs, frc, netClass, routeNumber, fow);
    }

    private Street extractStreet(Record record) {
        Map<String, Object> v = record.get("s").asMap();
        String sCord = (String) v.get(StreetFieldsNeo4j.COORDINATES);
        ArrayList<Coordinate> coordinates = getCoordinateList(sCord);
        long linkId = (long) v.get(StreetFieldsNeo4j.LINKID);
        long from = (long) v.get(StreetFieldsNeo4j.FROM);
        long to = (long) v.get(StreetFieldsNeo4j.TO);
        double length = (double) v.get(StreetFieldsNeo4j.LENGTH);
        int speedlimit = ((Long) v.get(StreetFieldsNeo4j.SPEEDLIMIT)).intValue();
        String areaname = (String) v.get(StreetFieldsNeo4j.AREANAME);
        String name = (String) v.get(StreetFieldsNeo4j.NAME);
        double weight = (double) v.get(StreetFieldsNeo4j.WEIGHT);
        double ffs = (double) v.get(StreetFieldsNeo4j.FFS);
        long frc = (long) v.get(StreetFieldsNeo4j.FRC);
        long netClass = (long) v.get(StreetFieldsNeo4j.NETCLASS);
        String routeNumber = (String) v.get(StreetFieldsNeo4j.ROUTENUMBER);
        long fow = (long) v.get(StreetFieldsNeo4j.FOW);

        return new Street(coordinates, linkId, from, to, length, speedlimit, areaname, name, weight, ffs, frc, netClass, routeNumber, fow);
    }

}
