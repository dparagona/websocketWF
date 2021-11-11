package data.Mongo;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import util.ConfigurationSingleton;


//TODO: handle replica set
public class MongoConnectionManager {
    ConfigurationSingleton conf = ConfigurationSingleton.getInstance();
//    public static final String DEFAULT_MONGO_HOSTNAME = "localhost";
//    public static final int DEFAULT_MONGO_PORT = 27017;
//    public static final String DATABASE_NAME = "promenade";
//    public static final String DEFAULT_COLLECTION_NAME = "areas";
//
    private final MongoClient mongo;

//    public MongoConnectionManager(String hostname, int port) {
//        String connectionString = "mongodb://" + hostname + ":" + port;
//        this.mongo = MongoClients.create(connectionString);
//    }
//
//    public MongoConnectionManager(String hostname) {
//        this(hostname, DEFAULT_MONGO_PORT);
//    }
//
//    public MongoConnectionManager() {
//        String hostname = System.getenv("MONGO_HOSTNAME");
//        if (hostname == null){
//            hostname = DEFAULT_MONGO_HOSTNAME;
//        }
//        String connectionString = "mongodb://" + hostname + ":" + DEFAULT_MONGO_PORT;
//        this.mongo = MongoClients.create(connectionString);
//    }


    public MongoConnectionManager() {
        String hostname = conf.getProperty("mongo.hostname");
        String port = conf.getProperty("mongo.port");
        String connectionString = "mongodb://" + hostname + ":" + port;
        this.mongo =  MongoClients.create(connectionString);
    }

    public MongoClient getClient() {
        return mongo;
    }

    public void close() {
        mongo.close();
    }
    
    public MongoDatabase getDatabase() {
    	CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
    	                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
    	return this.mongo.getDatabase(conf.getProperty("mongo.database.name")).withCodecRegistry(pojoCodecRegistry);
    }
    
    public <T> MongoCollection<T> getCollection(String collection, Class<T> documentClass) {
    	if(collection == null)
    		return this.getDatabase().getCollection(conf.getProperty("mongo.collection.name"), documentClass);
    	else
    		return this.getDatabase().getCollection(collection, documentClass);
    }
}