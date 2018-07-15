package cmcc.cmri.dgsq.run;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppSettings {

    private static final Logger logger = LogManager.getLogger(AppSettings.class);

    public static final Configuration config;

    static {

        // Init application configuration
        Configuration c;
        try {
            PropertiesConfiguration configs = new PropertiesConfiguration("application.properties");
            c = configs;

            logger.debug("Loading AppSettings");

        } catch (ConfigurationException cex) {
            logger.error("Cannot get application settings");
            c = null;
        }
        config = c;

        // Init Mongodb connection
    }
}
