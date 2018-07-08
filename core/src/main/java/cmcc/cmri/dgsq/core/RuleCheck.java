package cmcc.cmri.dgsq.core;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class RuleCheck {

    private static final Logger logger = LogManager.getLogger(RuleCheck.class);

    private XDR xdr;
    private MongoDatabase mongo;

    RuleCheck(String xdrFile) {

        // Phrase XDR from xdr file
        xdr = new XDR(xdrFile);

        // Connect to MongoDB
        mongo = MongoManager.getMongoDatabase(AppSettings.config.getString("mongo.db"));

        // Init Spark
    }

    private void run() {

        // Step 1. Load XDR file as DataSets, and cache() it for multiple uses

        // Step 2. Get all checks for the XDR
        MongoCursor<Document> cursor = mongo.getCollection("q_checks_f")
                .find(eq("interface_name", xdr.getName()))
                .iterator();

        try {
            while(cursor.hasNext()) {
                // Step 3. Get rule def
                logger.trace("Running rule[{}] on target[{}]");

                // Step 4. Run rule on file
            }

        } finally {
            cursor.close();
        }

        // Step 5. Close Spark
    }

    public static void main(String[] args) {

        // Check args
        if(args.length != 1) {
            usage();
            System.exit(1);
        }

        logger.trace("Starting RuleCheck on XDR file: [{}]", args[0]);

        RuleCheck ruleCheck = new RuleCheck(args[0]);
        ruleCheck.run();

        logger.trace("Finished.");
    }

    private static void usage() {
        logger.error("Usage: RuleCheck [xdr file]. eg: RuleCheck hdfs:///user/hadoop/xdr/http.txt");
    }

}
