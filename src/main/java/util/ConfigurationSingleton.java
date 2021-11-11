package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationSingleton {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationSingleton.class);
    private static final String JBOSS_SERVER_DATA_DIR = "jboss.server.data.dir";
    private final Properties properties;
    private static ConfigurationSingleton configurationSingleton;


    private ConfigurationSingleton() {

        properties = new Properties();
        try {
        File file = new File("/home/colarusso/Documents/GitRepository/Promenade/promenadeAreaNameService/conf.properties");
            if (!file.exists()) {
                file = new File("/opt/app-root/src/conf.properties");
            }
            if (!file.exists()) {
                file = new File(System.getProperty(JBOSS_SERVER_DATA_DIR) + File.separator + "conf.properties");
            }
            if (file.exists()) {
                properties.load(new FileInputStream(file));
            }
            else{
                throw new IOException();
            }
        } catch (IOException e) {
            LOGGER.debug("context",e);
        }
    }

    public static ConfigurationSingleton getInstance(){
        if(configurationSingleton == null){
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
