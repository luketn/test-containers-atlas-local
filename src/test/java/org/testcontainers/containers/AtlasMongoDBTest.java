package org.testcontainers.containers;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A base class you can extend in tests to run against an Atlas Local Development Environment.
 */
public abstract class AtlasMongoDBTest {
    private static final Logger log = getLogger(AtlasMongoDBTest.class);

    private static final Network network = Network.newNetwork();
    static MongoDAtlasContainer mongoDAtlas = new MongoDAtlasContainer("7.0-ubi8")
            .withLogConsumer(outputFrame -> log.debug("mongod: {}", outputFrame.getUtf8String()))
            .withNetwork(network);

    static MongoTAtlasContainer mongoTAtlas = new MongoTAtlasContainer()
            .withLogConsumer(outputFrame -> log.debug("mongot: {}", outputFrame.getUtf8String()))
            .withNetwork(network);

    // Singleton pattern Ref: https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    static {
        try {
            Instant start = Instant.now();

            log.debug("starting mongod...");
            mongoDAtlas.start();
            log.debug("mongod started, connection string: {}", mongoDAtlas.getConnectionString());

            log.debug("starting mongot...");
            mongoTAtlas.start();
            log.debug("mongot started");

            log.debug("waiting for mongod to see mongot is steady...");
            mongoDAtlas.waitForMongoTSteadyState();
            log.debug("mongod sees mongot in steady state");

            log.debug("pinging mongot from mongod...");
            boolean successResponse = mongoDAtlas.waitUntilMongoTPingSucceeds(Duration.ofSeconds(5));
            assertTrue("mongod could not ping mongot", successResponse);
            log.debug("mongod can ping mongot");

            //Check logs
            if (log.isTraceEnabled()) {
                try {
                    Files.writeString(Paths.get("./mongod-init-log.txt"), mongoDAtlas.getLogs());
                    Files.writeString(Paths.get("./mongot-init-log.txt"), mongoTAtlas.getLogs());
                } catch (IOException e) {
                    log.trace("An error occurred while writing mongod and mongot logs", e);
                }
            }

            log.debug("mongod and mongot started in {} seconds", Duration.between(start, Instant.now()).getSeconds());
        } catch (Exception e) {
            log.warn("An error occurred while starting mongod and mongot", e);
        }
    }

    protected String connectionString() {
        return mongoDAtlas.getConnectionString();
    }
}
