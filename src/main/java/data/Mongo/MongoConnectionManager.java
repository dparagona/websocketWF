package data.Mongo;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ConfigurationSingleton;


public class MongoConnectionManager {

    ConfigurationSingleton conf = ConfigurationSingleton.getInstance();

    private static final Logger logger = LoggerFactory.getLogger(MongoConnectionManager.class);

    private final MongoClient mongo;

    public MongoConnectionManager() {
        String hostname = conf.getProperty("mongo.hostname");
        String port = conf.getProperty("mongo.port");
        String connectionString = "mongodb://" + hostname + ":" + port;
        this.mongo = MongoClients.create(connectionString);
        logger.info("Created: " + this);
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
        if (collection == null)
            return this.getDatabase().getCollection(conf.getProperty("mongo.collection.name"), documentClass);
        else
            return this.getDatabase().getCollection(collection, documentClass);
    }

    @Override
    public String toString() {
        return "MongoConnectionManager{" +
                "conf=" + conf +
                ", mongo=" + mongo +
                '}';
    }
}