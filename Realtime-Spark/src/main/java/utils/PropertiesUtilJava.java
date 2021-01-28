package utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PropertiesUtilJava {
    public static Properties load(String propertieName) throws IOException {
        Properties prop = new Properties();
        prop.load(new InputStreamReader(ClassLoader.getSystemClassLoader().getResourceAsStream(propertieName), StandardCharsets.UTF_8));
        return prop;

        /**
         * How to use?
         *
         * Properties properties = PropertiesUtilJava.load("config.properties");
         * String property = properties.getProperty("elasticsearch.server");
         * System.out.println(property);
         */
    }
}
