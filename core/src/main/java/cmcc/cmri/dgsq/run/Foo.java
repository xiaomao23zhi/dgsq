package cmcc.cmri.dgsq.run;

import cmcc.cmri.dgsq.pojos.XDR;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class Foo {

    private static final Logger logger = LogManager.getLogger(Foo.class);

    // MongoDB connection
    private MongoDatabase mongo;

    private XDR xdr;

    Foo() {
        mongo = MongoManager.getMongoDatabase(AppSettings.config.getString("mongo.db"));
        xdr = new XDR("hdfs:///user/hadoop/dgsq/xdr/in/20180703/http_20180703150100_01_001_000.txt");
    }

    public void run() {
        MongoCursor<Document> checks = mongo.getCollection("q_checks")
                .find(and(eq("interface_name", "http"),eq("is_active", "1")))
                .iterator();
        try {
            while(checks.hasNext()) {
                // Step 3. Get rule def
                Document document = checks.next();
                String target = document.getString("check_target");
                String ruleId = document.getString("rule_id");
                String parms = document.getString("rule_params");

                Document rule = mongo.getCollection("q_rules")
                        .find(eq("rule_id", ruleId)).first();

                String rule_sql = rule.getString("rule_sql");

                String[] schema = AppSettings.config.getString("xdr.schema.http").split("\\|");
                int idx = -1;

                for(int i = 0; i < schema.length; i ++) {
                    if(target.equals(schema[i])) {
                        idx = i;
                    }
                }

                if(idx == -1) {
                    logger.error("Wrong filter column name [{}]", target);
                }

                logger.debug("schema: " + schema + "pos: " + idx);

                String filter = rule_sql
                        .replace("[target]", "line -> line.split(" + xdr.getDelimiter() + ")["+idx+"]")
                        .replace("[params]",parms);

                // Step 4. Run rule on file
                logger.trace("Running rule[{}] on target[{}] filter:[{}]", ruleId, target, filter);
            }

        } finally {
            checks.close();
        }
    }

    public void runSpark() {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("hdfs:///user/hadoop/people.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age unknown";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes);
        });
        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        peopleDataFrame.show();

        spark.close();

    }

    public static void main(String[] args) {

        logger.trace("APP VERSION:[{}]", AppSettings.config.getString("app.version"));

        Foo foo = new Foo();
        //foo.runSpark();
    }
}
