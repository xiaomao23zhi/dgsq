package cmcc.cmri.dgsq.run;

import cmcc.cmri.dgsq.pojos.XDR;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.spark.MongoSpark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RunCheck {
    // Define a static logger
    private static final Logger logger = LogManager.getLogger(RunCheck.class);
    // Dataset for xdr file
    JavaRDD<String> rdd;
    // XDR
    private XDR xdr;
    // XDR file
    private String xdrFile;
    // MongoDB connection
    private DB mongo;
    // Spark
    private SparkConf sc;
    // Spark-Mongo connector
    private JavaSparkContext jsc;
    // Spark SQL Context
    private SQLContext sqlContext;
    // File check result
    private String result;
    // File size
    private long length;
    // File xdrCount
    private long xdrCount;

    RunCheck(String xdrFile, String xdrSchema) {
        // Phrase XDR from xdr file, pattern: hdf:///[path]/[name]_yyyymmddHHMMSS_[vendor]_[device ID]_[sequence].txt
        this.xdrFile = xdrFile;
        xdr = new XDR(xdrFile, xdrSchema);

        //
        result = AppSettings.config.getString("check.file.ERR");
        length = 0L;
        xdrCount = 0L;

        // Connect to MongoDB
        mongo = MongoManager.getMongoDatabase(AppSettings.config.getString("mongo.db"));
        // Sparm-Mongo connector uri, mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
        String mongoInputUri;
        // Sparm-Mongo connector uri, mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
        String mongoOutputUri;
        String yyyymmdd = "a" + xdr.getDate().substring(0, 10).replace("-", "");

        // mongdodb://[mongo.host]:[mongo.port]/[mongo.db].[collection]
        mongoInputUri = "mongodb://"
                + AppSettings.config.getString("mongo.user") + ":"
                + AppSettings.config.getString("mongo.pass") + "@"
                + AppSettings.config.getString("mongo.host") + ":"
                + AppSettings.config.getInt("mongo.port") + "/"
                + AppSettings.config.getString("mongo.db") + "."
                + "q_checks"
                + "?authSource=admin";
        // For abnormal data writing to Mongo
        mongoOutputUri = "mongodb://"
                + AppSettings.config.getString("mongo.user") + ":"
                + AppSettings.config.getString("mongo.pass") + "@"
                + AppSettings.config.getString("mongo.host") + ":"
                + AppSettings.config.getInt("mongo.port") + "/"
                + AppSettings.config.getString("mongo.db") + "."
                + "q_results_r"
                + "?authSource=admin";

        logger.trace("Build SparkSession with urls: [{}] [{}]", mongoInputUri, mongoOutputUri);

        // Init Spark
        sc = new SparkConf()
                .setAppName("dgsq - RunCheck")
                .set("spark.mongodb.input.uri", mongoInputUri)
                .set("spark.mongodb.output.uri", mongoOutputUri);

        jsc = new JavaSparkContext(sc);
        sqlContext = new SQLContext(jsc);
    }

    // Main
    public static void main(final String[] args) {

        // Check args
        if (args.length != 2) {
            usage();
            System.exit(1);
        }

        logger.trace("Starting RunCheck on XDR file: [{}]", args[0]);

        RunCheck runCheck = new RunCheck(args[0], args[1]);

        runCheck.init();
        runCheck.fileCheck();
        runCheck.ruleCheck();

        // Step 5. Close Spark
        runCheck.release();

        logger.trace("Finished.");
    }

    public static void usage() {
        logger.error("Usage: RunCheck [xdr file] [xdr schema]");
        logger.error("  eg: eg: RunCheck hdfs:///user/hadoop/xdr/http.txt col1,col2,col3");
    }

    // Load xdr file
    private void init() {

        logger.trace("Loading XDR file to Spark.");

        //rdd = spark.read().textFile(xdrFile).cache();
        rdd = jsc.textFile(xdrFile);
        result = AppSettings.config.getString("check.file.ERR");


        // Clean q_result_f and q_result_r
        logger.trace("Clean q_result_f and q_result_r records of XDR: [{}]", xdr.getFile());
        BasicDBObject filter = new BasicDBObject("file_name",xdr.getFile());
        MongoManager.deleteMany(AppSettings.config.getString("mongo.db"), "q_results_f", filter);
        MongoManager.deleteMany(AppSettings.config.getString("mongo.db"), "q_results_r", filter);
    }

    //
    private void release() {
        jsc.close();
    }

    // Check xdr file
    private void fileCheck() {

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

        logger.debug("Calculating file xdrCount: " + xdrFile);
        xdrCount = rdd.count();

        logger.debug("File size: [{}], File xdrCount: [{}]", +length, xdrCount);

        if (length == 0) {
            result = AppSettings.config.getString("check.file.NULL");
        } else {
            result = AppSettings.config.getString("check.file.OK");
        }

        // to-do: check if file struct matches schema info

        // Write to MongoDB
        // MongoCollection<Document> collection = mongo.getCollection("q_results_f");

        BasicDBObject document = new BasicDBObject();
        document.put("dpi_vendor", xdr.getVendor() + "_" + xdr.getDevice());
        document.put("interface_name", xdr.getInerface());
        document.put("file_name", xdr.getFile());
        document.put("file_date", xdr.getDate());
        document.put("file_size", length);
        document.put("record_count", xdrCount);
        document.put("check_result", result);
        document.put("timestamp", new Date());

        // Insert new
        MongoManager.insert(AppSettings.config.getString("mongo.db"), "q_results_f", document);

        //logger.trace("Logging to MongoDB");
        //collection.insertOne(document);
    }

    // Check rule upon xdr file
    private void ruleCheck() {

        // Abnormal xdrCount
        long abnCount;
        // MongoDB connector
        DBCursor checks = null;

        String delimiter = xdr.getDelimiter();

        try {
            // Step 1. Generate the schema based on the string of schema
            logger.trace("Generate XDR schema");
            List<StructField> fields = new ArrayList<>();
            for (String fieldName : xdr.getSchemas().split(",")) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);
            // Convert records of the RDD (people) to Rows
            JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) record -> {
                String[] attributes = record.split(delimiter);
                return RowFactory.create(attributes);
            });

            // Apply the schema to the RDD
            DataFrame df = sqlContext.createDataFrame(rowRDD, schema);

            // Creates a temporary view using the DataFrame
            df.registerTempTable(xdr.getInerface());
            df.persist(StorageLevel.MEMORY_AND_DISK());
            //// Convert to Parquet for better performance
            //logger.debug("Convert to Parquet");
            //ds.write().mode(SaveMode.Overwrite).parquet(xdr.getName() + ".parquet");
            //Dataset<Row> dsParquet = spark.read().parquet(xdr.getName() + ".parquet");
            //dsParquet.createOrReplaceTempView(xdr.getName());
            //dsParquet.schema();

            logger.debug("Show schema");
            df.show();

            // Step 2. Get all active checks for the XDR
            BasicDBObject filter = new BasicDBObject("interface_name",xdr.getInerface())
                    .append("is_active", "1");
            checks = MongoManager.find(AppSettings.config.getString("mongo.db"), "q_checks", filter);

            while (checks.hasNext()) {
                // Step 3. Get rule def
                DBObject check = checks.next();
                String ruleId = (String) check.get("rule_id");
                String ruleParams = (String) check.get("rule_params");
                String checkId = (String) check.get("check_id");
                String checkTarget = (String) check.get("check_target");
                String checkName = (String) check.get("check_name");
                String yyyymmdd = "a" + xdr.getDate().substring(0, 10).replace("-", "");

                // Get rule_sql
                /*
                switch (ruleId) {
                    case "201":
                        // NULL check
                        abnormal = rdd.filter(line -> line.split(delimiter)[idx].isEmpty());
                        break;
                    case "202":
                        // Zero check
                        abnormal = rdd.filter(line -> "0".equals(line.split(delimiter)[idx]));
                        break;
                    case "203":
                        // Range check
                        abnormal = rdd.filter(line -> !ruleParams.contains(line.split(delimiter)[idx]));
                        break;
                    case "204":
                        // Value check, don't know what to do this yet...
                        break;
                    default:
                        logger.error("Unsupported check rule");
                }
                */
                String ruleSql = (String) MongoManager.findOne(AppSettings.config.getString("mongo.db"), "q_rules", "rule_id", ruleId)
                        .get("rule_sql");
                // SELECT * FROM [table] WHERE [target] != null
                ruleSql = ruleSql.replace("[table]", xdr.getInerface()).replace("[target]", checkTarget).replace("[params]",ruleParams);
                logger.debug("rule_sql is {}", ruleSql);

                // Step 4. Run rule on file
                DataFrame abnormal = sqlContext.sql(ruleSql);

                logger.debug("Counting abnormal counts");
                if (abnormal != null) {
                    abnCount = abnormal.count();
                } else {
                    abnCount = -1;
                }

                logger.trace("Finished check rule[{}] on target[{}] with params[{}], abnormal counts[{}]",
                        ruleId, checkTarget, ruleParams, abnCount);

                DataFrame abnLimit = abnormal.limit(AppSettings.config.getInt("xdr.abn.limit"));

                //MongoSpark.write(abnLimit).mode("overwrite").save();
                logger.trace("Writing abnormal data to MongoDB: {}.{}", yyyymmdd, xdr.getName() + checkId);
                MongoSpark.write(abnLimit).option("database", yyyymmdd).option("collection", xdr.getName() + checkId).mode("overwrite").save();

                // Write to MongoDB
                //MongoCollection<Document> collection = mongo.getCollection("q_results_r");

                BasicDBObject document = new BasicDBObject();
                document.put("dpi_vendor", xdr.getVendor() + "_" + xdr.getDevice());
                document.put("interface_name", xdr.getInerface());
                document.put("file_name", xdr.getFile());
                document.put("file_date", xdr.getDate());
                document.put("check_name", checkName);
                document.put("check_target", checkTarget);
                document.put("check_id", checkId);
                document.put("check_counts", xdrCount);
                document.put("abnormal_counts", abnCount);
                document.put("abnormal_data", yyyymmdd + "." + xdr.getName() + checkId);
                document.put("timestamp", new Date());

                MongoManager.insert(AppSettings.config.getString("mongo.db"), "q_results_r", document);

                //logger.trace("Logging to MongoDB: {}", document);
                //collection.insertOne(document);
            }

        } finally {
            jsc.close();
            if (checks != null) {
                checks.close();
            }
        }
    }
}
