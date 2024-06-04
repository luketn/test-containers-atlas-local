package com.mycodefu;

import org.junit.Test;
import org.testcontainers.containers.AtlasMongoDBTest;

import java.time.Instant;

import static org.junit.Assert.*;

public class DataAccessTest extends AtlasMongoDBTest {

    @Test
    public void testMain() throws Exception {
        String atlasSearchTestcontainersConnectionString = super.connectionString();
        try (DataAccess dataAccess = new DataAccess(atlasSearchTestcontainersConnectionString, "test", "test")) {
            dataAccess.initAtlasSearchIndex();

            dataAccess.insertData(new DataAccess.TestData("test", 123, true));

            DataAccess.TestData foundRegular = dataAccess.findClassic(123);
            assertNotNull("Failed to find using classic find()", foundRegular);

            //Wait for Atlas Search to index the data (Atlas Search is eventually consistent)
            Instant start = Instant.now();
            DataAccess.TestData foundSearch = null;
            while (Instant.now().isBefore(start.plusSeconds(5))) {
                foundSearch = dataAccess.findAtlasSearch(123);
                if (foundSearch != null) {
                    break;
                }
                Thread.sleep(10);
            }
            assertNotNull("Failed to find using Atlas Search", foundSearch);
        }
    }
}