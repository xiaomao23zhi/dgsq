package cmcc.cmri.dgsq.core;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.IOException;
import java.util.Date;

public class FileCheck {

    // Define a static logger
    private static final Logger logger = LogManager.getLogger(FileCheck.class);

    // XDR
    private XDR xdr;

    // XDR file
    private String xdrFile;

    // MongoDB connection
    private MongoDatabase mongo;

    // File check result
    private String result;

    // File size
    private long length;

    // File count
    private long count;

    FileCheck(String xdrFile) {

        // Phrase XDR from xdr file, pattern: hdf:///[path]/[name]_yyyymmddHHMMSS_[vendor]_[device ID]_[sequence].txt
        this.xdrFile = xdrFile;
        xdr = new XDR(xdrFile);

        // Connect to MongoDB
        mongo = MongoManager.getMongoDatabase(AppSettings.config.getString("mongo.db"));

        result =  AppSettings.config.getString("check.file.ERR");
        length = 0L;
        count = 0L;
    }

    public void run() {

        logger.debug("Calculating file size: " + xdrFile);

        // Use HDFS to get file size
        Configuration config = new Configuration();
        Path path = new Path(xdrFile);
        try {

            FileSystem hdfs = path.getFileSystem(config);
            ContentSummary summary = hdfs.getContentSummary(path);
            length = summary.getLength();

        } catch (IOException e) {
            logger.error("File not found: " + xdrFile);
            result = AppSettings.config.getString("check.file.MISS");
            return;
        }

        logger.debug("Calculating file count: " + xdrFile);

        // User Spark to get file counts
        SparkSession spark = SparkSession.builder()
                .appName("dgsq.FileCheck")
                .getOrCreate();

        Dataset<String> ds = spark.read().textFile(xdrFile).cache();
        count = ds.count();

        spark.close();

        logger.debug("File size: [{}], File count: [{}]", + length, count);

        if(length == 0) {
            result = AppSettings.config.getString("check.file.NULL");
        } else {
            result = AppSettings.config.getString("check.file.OK");
        }
    }

    public void logToMongo() {

        // Write to MongoDB
        MongoCollection<Document> collection = mongo.getCollection("q_results_f");

        Document document = new Document();
        document.put("dpi_vendor", xdr.getVendor() + "_" + xdr.getDevice());
        document.put("interface_name", xdr.getName());
        document.put("file_name", xdr.getFile());
        document.put("file_date", xdr.getDate());
        document.put("file_size", length);
        document.put("record_count", count);
        document.put("check_result", result);
        document.put("timestamp", new Date());

        logger.trace("Logging to MongoDB");

        collection.insertOne(document);
    }

    public static void main(final String[] args) {

        // Check args
        if (args.length != 1) {
            usage();
            System.exit(1);
        }

        logger.trace("Starting FileCheck on XDR file: [{}]", args[0]);

        FileCheck fileCheck = new FileCheck(args[0]);
        fileCheck.run();
        fileCheck.logToMongo();

        logger.trace("Finished.");
    }


    public static void usage() {
        logger.error("Usage: RuleCheck [xdr file]. eg: FileCheck hdfs:///user/hadoop/xdr/http.txt");
    }
}
