package com.mycodefu;

import com.mongodb.client.*;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.slf4j.LoggerFactory.getLogger;

public class DataAccess implements AutoCloseable {
    private static final Logger log = getLogger(DataAccess.class);

    public record TestData(String test, int test2, boolean test3) { }

    private final MongoClient mongoClient;
    private final MongoCollection<TestData> testCollection;

    public DataAccess(String connectionString, String databaseName, String collectionName) {
        log.info("DataAccess connecting to {}", connectionString);

        mongoClient = MongoClients.create(connectionString);
        var testDB = mongoClient.getDatabase(databaseName);
        testDB.createCollection(collectionName);
        testCollection = testDB.getCollection(collectionName, TestData.class);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    public void initAtlasSearchIndex() {
        testCollection.createSearchIndex("AtlasSearchIndex",
                BsonDocument.parse("""
                        {
                          "mappings": {
                            "dynamic": false,
                            "fields": {
                              "test2": {
                                "type": "number",
                                "representation": "int64",
                                "indexDoubles": false
                              },
                              "test": {
                                "type": "string"
                              },
                              "test3": {
                                "type": "boolean"
                              }
                            }
                          }
                        }""")
        );

        //wait for the search index to be ready
        boolean ready = false;
        while (!ready) {
            ListSearchIndexesIterable<Document> searchIndexes = testCollection.listSearchIndexes();
            for (Document searchIndex : searchIndexes) {
                if (searchIndex.get("name").equals("AtlasSearchIndex")) {
                    ready = searchIndex.get("status").equals("READY");
                    if (ready) {
                        System.out.println("Search index AtlasSearchIndex is ready");
                        break;
                    }
                }
            }
        }
    }
    
    public void insertData(TestData data) {
        log.info("Inserting document {}", data);
        testCollection.insertOne(data);
    }
    
    public TestData findClassic(int test2) {
        return testCollection.find(eq("test2", test2)).first();
    }
    
    public TestData findAtlasSearch(int test2) {
        List<Document> query = Arrays.asList(new Document("$search",
                        new Document("index", "AtlasSearchIndex")
                                .append("equals",
                                        new Document()
                                                .append("path", "test2")
                                                .append("value", test2)
                                )
                )
        );
        return testCollection.aggregate(query).first();
    }
}