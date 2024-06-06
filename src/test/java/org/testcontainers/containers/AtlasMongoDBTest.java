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
 * Ref: https://github.com/mongodb/mongodb-atlas-cli/blob/master/internal/cli/deployments/setup.go
 */
public abstract class AtlasMongoDBTest {
    private static final Logger log = getLogger(AtlasMongoDBTest.class);

    static MongoDAtlasLocalContainer mongoDAtlas = new MongoDAtlasLocalContainer()
            .withLogConsumer(outputFrame -> log.debug("mongod: {}", outputFrame.getUtf8String()));

    // Singleton pattern Ref: https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    static {
        try {
            Instant start = Instant.now();

            log.debug("starting mongod...");
            mongoDAtlas.start();
            log.debug("mongod started, connection string: {}", mongoDAtlas.getConnectionString());

            //Check logs
            if (log.isTraceEnabled()) {
                try {
                    Files.writeString(Paths.get("./mongod-init-log.txt"), mongoDAtlas.getLogs());
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
