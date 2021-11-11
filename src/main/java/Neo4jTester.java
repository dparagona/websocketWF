import data.neo4j.Neo4jDAOImpl;

public class Neo4jTester {


    public static void main(String[] args) {
        Neo4jDAOImpl neo4jDAO = new Neo4jDAOImpl("bolt://localhost:7687", "neo4j", "password");
        neo4jDAO.openConnection();
        neo4jDAO.getStreetFromArea("Lentilly", 18);
    }
}
