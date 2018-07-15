package cmcc.cmri.dgsq.run;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Date;
import static com.mongodb.client.model.Filters.eq;


public class MongoManager {

    private static final Logger logger = LogManager.getLogger(MongoManager.class);

    private static MongoClient client;

    static {
        try {

            client = new MongoClient(
                    AppSettings.config.getString("mongo.host"),
                    AppSettings.config.getInt("mongo.port"));

            logger.trace("Connected to MongoDB: {}", client.toString());
        } catch (Exception e) {
            client = null;

            logger.error("Could not connect to MongoDB");
            e.printStackTrace();
        }
    }

    // Get MongoDB
    public static MongoDatabase getMongoDatabase(String db) {
        return client.getDatabase(db);
    }

    // Get MongoCollection
    public static MongoCollection getMongoCollection(String db, String col) {
        return client.getDatabase(db).getCollection(col);
    }

    // Write to application run log
    public static void writeRunLog(String appName, String runStatus, String message) {
        MongoCollection runLog = client.getDatabase(AppSettings.config.getString("mongo.db"))
                .getCollection("q_run_log");
        Document document = new Document("application_name", appName)
                .append("", "")
                .append("time_stamp", new Date());
        runLog.insertOne(document);
    }

    // Insert document
    public static void insert(String db, String name, Document document) {
        logger.trace("Write to MongoDB: {}/{}.{}", db, name, document);
        MongoCollection collection = client.getDatabase(db).getCollection(name);
        collection.insertOne(document);
    }

    // Find document with filter: key = value<String>
    public static MongoCursor<Document> find(String db, String collection, String key, String value) {
        Bson filter = eq(key, value);
        return client.getDatabase(db).getCollection(collection)
                .find(filter).iterator();
    }

    // Find document with filter: key = value<int>
    public static MongoCursor<Document> find(String db, String collection, String key, int value) {
        Bson filter = eq(key, value);
        return client.getDatabase(db).getCollection(collection)
                .find(filter).iterator();
    }

    // Close connection
    public static void close() {
        client.close();
    }
}
