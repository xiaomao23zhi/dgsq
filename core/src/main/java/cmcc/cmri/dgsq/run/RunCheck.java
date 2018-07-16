package cmcc.cmri.dgsq.run;

import cmcc.cmri.dgsq.pojos.XDR;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class RunCheck {
    // Define a static logger
    private static final Logger logger = LogManager.getLogger(RunCheck.class);
    // Dataset for xdr file
    JavaRDD<String> df;
    // XDR
    private XDR xdr;
    // XDR file
    private String xdrFile;
    // MongoDB connection
    private MongoDatabase mongo;
    // Spark
    private SparkSession spark;
    // Spark-Mongo connector
    private JavaSparkContext jsc;
    // File check result
    private String result;
    // File size
    private long length;
    // File xdrCount
    private long xdrCount;

    RunCheck(String xdrFile) {
        // Phrase XDR from xdr file, pattern: hdf:///[path]/[name]_yyyymmddHHMMSS_[vendor]_[device ID]_[sequence].txt
        this.xdrFile = xdrFile;
        xdr = new XDR(xdrFile);

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
        String yyyymmdd = xdr.getDate().substring(0, 10).replace("-", "");

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
                + yyyymmdd;

        logger.trace("Build SparkSession with urls: [{}] [{}]", mongoInputUri, mongoOutputUri);

        // Init Spark
        spark = SparkSession.builder().appName("dgsq - RunCheck")
                .config("spark.mongodb.input.uri", mongoInputUri)
                .config("spark.mongodb.output.uri", mongoOutputUri)
                .getOrCreate();
        jsc = new JavaSparkContext(spark.sparkContext());
    }

    // Main
    public static void main(final String[] args) {

        // Check args
        if (args.length != 1) {
            usage();
            System.exit(1);
        }

        logger.trace("Starting RunCheck on XDR file: [{}]", args[0]);

        RunCheck runCheck = new RunCheck(args[0]);

        runCheck.init();
        runCheck.fileCheck();
        runCheck.ruleCheck();

        // Step 5. Close Spark
        runCheck.release();

        logger.trace("Finished.");
    }

    public static void usage() {
        logger.error("Usage: RunCheck [xdr file]. eg: RunCheck hdfs:///user/hadoop/xdr/http.txt");
    }

    // Load xdr file
    private void init() {

        logger.trace("Loading XDR file to Spark.");

        //df = spark.read().textFile(xdrFile).cache();
        df = spark.sparkContext()
                .textFile(xdrFile, 8)
                .toJavaRDD().cache();
        result = AppSettings.config.getString("check.file.ERR");
    }

    //
    private void release() {
        spark.close();
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
        xdrCount = df.count();

        logger.debug("File size: [{}], File xdrCount: [{}]", +length, xdrCount);

        if (length == 0) {
            result = AppSettings.config.getString("check.file.NULL");
        } else {
            result = AppSettings.config.getString("check.file.OK");
        }

        // to-do: check if file struct matches schema info

        // Write to MongoDB
        // MongoCollection<Document> collection = mongo.getCollection("q_results_f");

        Document document = new Document();
        document.put("dpi_vendor", xdr.getVendor() + "_" + xdr.getDevice());
        document.put("interface_name", xdr.getName());
        document.put("file_name", xdr.getFile());
        document.put("file_date", xdr.getDate());
        document.put("file_size", length);
        document.put("record_count", xdrCount);
        document.put("check_result", result);
        document.put("timestamp", new Date());

        MongoManager.insert(AppSettings.config.getString("mongo.d"), "q_results_f", document);

        //logger.trace("Logging to MongoDB");
        //collection.insertOne(document);
    }

    // Check rule upon xdr file
    private void ruleCheck() {

        // Abnormal xdrCount
        long abnCount;
        // MongoDB connector
        MongoCursor<Document> checks = null;

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
            JavaRDD<Row> rowRDD = df.map((Function<String, Row>) record -> {
                String[] attributes = record.split(xdr.getDelimiter());
                return RowFactory.create(attributes);
            });

            // Apply the schema to the RDD
            Dataset<Row> ds = spark.createDataFrame(rowRDD, schema);

            //// Creates a temporary view using the DataFrame
            //ds.createOrReplaceTempView(xdr.getName());
            // Convert to Parquet for better performance
            ds.write().parquet(xdr.getName() + ".parquet");
            Dataset<Row> dsParquet = spark.read().parquet(xdr.getName() + ".parquet");
            dsParquet.createOrReplaceTempView(xdr.getName());

            // Step 2. Get all active checks for the XDR
            checks = mongo.getCollection("q_checks")
                    .find(and(eq("interface_name", xdr.getName()), eq("is_active", "1")))
                    .iterator();

            while (checks.hasNext()) {
                // Step 3. Get rule def
                Document check = checks.next();
                String ruleId = check.getString("rule_id");
                String ruleParams = check.getString("rule_params");
                String checkId = check.getString("check_id");
                String checkTarget = check.getString("check_target");
                String yyyymmdd = xdr.getDate().substring(0, 10).replace("-", "");

                // Get rule_sql
                String ruleSql = mongo.getCollection("q_rules")
                        .find(eq("rule_id", ruleId)).first()
                        .getString("rule_sql");

                // SELECT * FROM [table] WHERE [target] != null
                ruleSql.replace("table", xdr.getName()).replace("target", checkTarget);
                logger.debug("rule_sql is {}", ruleSql);

                // Step 4. Run rule on file
                Dataset<Row> abnormal = spark.sql(ruleSql);

                logger.debug("Counting abnormal counts");
                if (abnormal != null) {
                    abnCount = abnormal.count();
                } else {
                    abnCount = -1;
                }

                logger.trace("Finished check rule[{}] on target[{}] with params[{}], abnormal counts[{}]",
                        ruleId, checkTarget, ruleParams, abnCount);

                Dataset<Row> abnLimit = abnormal.limit(AppSettings.config.getInt("xdr.abn,limit"));

                //MongoSpark.write(abnLimit).mode("overwrite").save();
                logger.trace("Writing abnormal data to MongoDB: {}.{}", yyyymmdd, checkId);
                MongoSpark.write(abnLimit).option("database", yyyymmdd).option("collection", checkId).mode("overwrite").save();

                // Write to MongoDB
                //MongoCollection<Document> collection = mongo.getCollection("q_results_r");

                Document document = new Document();
                document.put("dpi_vendor", xdr.getVendor() + "_" + xdr.getDevice());
                document.put("interface_name", xdr.getName());
                document.put("file_name", xdr.getFile());
                document.put("file_date", xdr.getDate());
                document.put("check_target", checkTarget);
                document.put("check_id", checkId);
                document.put("check_counts", xdrCount);
                document.put("abnormal_counts", abnCount);
                document.put("abnormal_data", yyyymmdd + "." + checkId);
                document.put("timestamp", new Date());

                MongoManager.insert(AppSettings.config.getString("mongo.db"), "q_results_r", document);

                //logger.trace("Logging to MongoDB: {}", document);
                //collection.insertOne(document);
            }

        } finally {
            jsc.close();
            spark.close();
            if (checks != null) {
                checks.close();
            }
        }
    }
}
