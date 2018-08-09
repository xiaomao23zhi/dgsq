package cmcc.cmri.dgsq.run;

import com.mongodb.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;


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
    public static DB getMongoDatabase(String db) {
        return client.getDB(db);
    }

    // Get MongoCollection
    public static DBCollection getMongoCollection(String db, String col) {
        return client.getDB(db).getCollection(col);
    }

    // Write to application run log
    public static void writeRunLog(String appName, String runStatus, String message) {
        DBCollection runLog = client.getDB(AppSettings.config.getString("mongo.db"))
                .getCollection("q_run_log");
        BasicDBObject document = new BasicDBObject("application_name", appName)
                .append("", "")
                .append("time_stamp", new Date());
        runLog.insert(document);
    }

    // Insert document
    public static void insert(String db, String name, BasicDBObject document) {
        logger.trace("Write to MongoDB: {}/{}.{}", db, name, document);
        DBCollection collection = client.getDB(db).getCollection(name);
        collection.insert(document);
    }

    // Find document with filter
    public static DBCursor find(String db, String collection, BasicDBObject filter) {
        return client.getDB(db).getCollection(collection).find(filter);
    }

    // Find document with filter: key = value<String>
    public static DBCursor find(String db, String collection, String key, String value) {
        BasicDBObject filter = new BasicDBObject(key, value);
        return client.getDB(db).getCollection(collection)
                .find(filter);
    }

    // Find document with filter: key = value<int>
    public static DBCursor find(String db, String collection, String key, int value) {
        BasicDBObject filter = new BasicDBObject(key, value);
        return client.getDB(db).getCollection(collection)
                .find(filter);
    }

    // Find one document with filter: key = value<String>
    public static DBObject findOne(String db, String collection, String key, String value) {
        BasicDBObject filter = new BasicDBObject(key, value);
        return client.getDB(db).getCollection(collection)
                .findOne(filter);
    }

    public static void deleteMany(String db, String collection, BasicDBObject filter) {
        client.getDB(db).getCollection(collection).remove(filter);
    }

    // Close connection
    public static void close() {
        client.close();
    }
}
