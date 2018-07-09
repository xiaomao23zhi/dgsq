package cmcc.cmri.dgsq.core;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class RuleCheck {

    // Define a static logger
    private static final Logger logger = LogManager.getLogger(RuleCheck.class);

    // XDR
    private XDR xdr;

    // XDR file
    private String xdrFile;

    // MongoDB connection
    private MongoDatabase mongo;

    // Sparm-Mongo connector uri, mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
    private String mongoInputUri;

    // Sparm-Mongo connector uri, mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
    private String mongoOutputUri;

    // Spark
    private SparkSession spark;

    RuleCheck(String xdrFile) {

        // Phrase XDR from xdr file, pattern: hdf:///[path]/[name]_yyyymmddHHMMSS_[vendor]_[device ID]_[sequence].txt
        this.xdrFile = xdrFile;
        xdr = new XDR(xdrFile);

        // Connect to MongoDB
        mongo = MongoManager.getMongoDatabase(AppSettings.config.getString("mongo.db"));

        // mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
        mongoInputUri = "mongodb://"
                + AppSettings.config.getString("mongo.host") + ":"
                + AppSettings.config.getInt("mongo.port") + "/"
                + AppSettings.config.getString("mongo.db") + "."
                + "q_checks";

        // For abnormal data writing to Mongo
        mongoOutputUri = "mongodb://"
                + AppSettings.config.getString("mongo.host") + ":"
                + AppSettings.config.getInt("mongo.port") + "/"
                + AppSettings.config.getString("mongo.db") + "."
                + xdr.getDate();

        // Init Spark
        spark = SparkSession.builder().appName("dgsq - RuleCheck")
                .config("spark.mongodb.input.uri", mongoInputUri)
                .config("spark.mongodb.output.uri", mongoOutputUri)
                .getOrCreate();
    }

    private void run() {

        // Step 1. Load XDR file as DataSets, and cache() it for multiple uses
        logger.trace("Loading XDR file to Spark");

        Dataset<String> df = null;
        df = spark.read().textFile(xdrFile).cache();

        // Step 2. Get all checks for the XDR
        MongoCursor<Document> cursor = mongo.getCollection("q_checks_f")
                .find(eq("interface_name", xdr.getName()))
                .iterator();
        try {
            while(cursor.hasNext()) {
                // Step 3. Get rule def


                // Step 4. Run rule on file
                logger.trace("Running rule[{}] on target[{}]");
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
