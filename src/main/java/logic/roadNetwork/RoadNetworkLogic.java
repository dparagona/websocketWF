package logic.roadNetwork;

import data.model.Coordinate;
import data.model.Street;
import data.neo4j.Neo4jDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ConfigurationSingleton;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Stateless;
import java.util.ArrayList;

@Stateless
public class RoadNetworkLogic implements RoadNetworkLogicLocal {

    ConfigurationSingleton conf = ConfigurationSingleton.getInstance();

    private static final Logger logger = LoggerFactory.getLogger(RoadNetworkLogic.class);
    Neo4jDAOImpl database;

    public RoadNetworkLogic() {
        String uri = conf.getProperty("neo4j-core.bolt-uri");
        String user = conf.getProperty("neo4j-core.user");
        String password = conf.getProperty("neo4j-core.password");
        database = new Neo4jDAOImpl(uri, user, password);
    }

    /**
     * Called after the EJB construction.
     * Open the connection to the database.
     */
    @PostConstruct
    public void connect() {
        logger.info("TrafficMonitoringService.connect");
        database.openConnection();
    }

    /**
     * Called before the EJB destruction.
     * Close the connection to the database.
     */
    @PreDestroy
    public void preDestroy() {
        logger.info("TrafficMonitoringService.preDestroy");
        database.closeConnection();
    }

    @Override
    public ArrayList<Street> getStreetFromArea(String areaname, int zoom, int decimateSkip) {

        ArrayList<Street> streets = database.getStreetFromArea(areaname, zoom);
        if (decimateSkip > 0) {
            for (Street s : streets) {
                ArrayList<Coordinate> geometry = s.getGeometry();
                ArrayList<Coordinate> geometryFiltered = new ArrayList<>();
                int geometrySize = geometry.size();
                for (int i = 0; i < geometrySize; i = i + decimateSkip + 1) {
                    geometryFiltered.add(geometry.get(i));
                }
                if (geometrySize % (decimateSkip + 1)!= 0)
                    geometryFiltered.add(geometry.get(geometrySize-1));
                s.setGeometry(geometryFiltered);
                logger.info("Decimation of street: " + geometrySize + "/" + geometryFiltered.size());
            }
        }

        return streets;
    }
}
