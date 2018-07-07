package cmcc.cmri.dgsq.core;


import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class AppSettings {

    public static final Configuration config;
    private static final Logger logger = LogManager.getLogger(AppSettings.class);

    static {

        // Init application configuration
        Configuration c = null;
        try {
            Configurations configs = new Configurations();
            c = configs.properties(new File("application.properties"));

            logger.debug("Loading AppSettings");

        } catch (ConfigurationException cex) {
            logger.error("Cannot get application settings");
            c = null;
        }
        config = c;

        // Init Mongodb connection
    }
}
