package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


public class ConfigurationSingleton {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationSingleton.class);
    private final Properties properties;
    private static ConfigurationSingleton configurationSingleton;

    private ConfigurationSingleton() {
        properties = new Properties();
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Created ConfigurationSingleton");
//        System.out.println("properties = " + properties.getProperty("neo4j-core.bolt-uri"));
    }

    public static ConfigurationSingleton getInstance() {
        if (configurationSingleton == null) {
            configurationSingleton = new ConfigurationSingleton();
        }
        return configurationSingleton;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultVal) {
        return properties.getProperty(key, defaultVal);
    }

    public Properties getProperties() {
        return properties;
    }
}
