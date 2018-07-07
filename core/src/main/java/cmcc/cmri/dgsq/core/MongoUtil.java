package cmcc.cmri.dgsq.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;


public class MongoUtil {

    private static final Logger logger = LogManager.getLogger(MongoUtil.class);

    public static MongoDatabase database;

    static {
        try {
            MongoClient mongoClient = new MongoClient(AppSettings.config.getString("mongo.host"),
                    AppSettings.config.getInt("mongo.port"));

            database = mongoClient.getDatabase(AppSettings.config.getString("mongo.db"));

            logger.trace("Connected to MongoDB: mongo://{}:{}/{}", database.toString());
        } catch (Exception e) {
            database = null;

            logger.error("Could not connect to MongoDB");
            e.printStackTrace();
        }
    }

    // Write to MongoDB
    public static void writeToMongo(String collection, Document document) {
        MongoCollection<Document> col = database.getCollection(collection);
        col.insertOne(document);

        logger.trace("Writing to MongoDB: {}:{}", col, document.toString());
    }
}
