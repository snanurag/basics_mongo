package mongo;

/**
 * Do the find operation on mongodbshell after running this function with and without index. There would be huge time difference.
 */

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IndexedCollection {

    static boolean keepIndexed = false;

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> phone_num = database.getCollection("phone_num");
        long records = phone_num.count();

        IndexOptions options = new IndexOptions();
        options.name("number");
        if (keepIndexed)
            phone_num.createIndex(Indexes.ascending("number"), options);
        else
            phone_num.dropIndex("number");

        System.out.println("Index is created.");

        //Create around 2 billion entries on indexed collection.
        if (records < Integer.MAX_VALUE) {
            createRecord(phone_num);
        }

    }


    public static void createRecord(MongoCollection<Document> collection) {
        Random r = new Random();

        //20 M entries are sufficient to show the importance of index
        long count = collection.count();
        while (count < Integer.MAX_VALUE) {
            List<Document> documents = new ArrayList<>();
            long time = System.currentTimeMillis();
            for (int i = 0; i < 10000000; i++) {
                int n = r.nextInt();
                documents.add(new Document("number", n < 0 ? n * -1 : n));
            }
            collection.insertMany(documents);
            System.out.println(System.currentTimeMillis()-time);
            count = collection.count();
            System.out.println("Total records as of now " + count);
        }
        collection.insertOne(new Document("number", 9968525380L));

    }

}
