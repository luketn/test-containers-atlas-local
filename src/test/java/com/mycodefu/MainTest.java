package com.mycodefu;

import org.junit.Test;
import org.testcontainers.containers.AtlasMongoDBTest;

import static org.junit.Assert.*;

public class MainTest extends AtlasMongoDBTest {

    @Test
    public void testMain() {
        System.out.println("MongoDB is ready to connect to for testing at " + super.connectionString());
    }

}