package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testcontainers.containers.MongoAtlasConstants.shared_key_file_content;

public class MongoDAtlasContainer extends GenericContainer<MongoDAtlasContainer> {
    private static final Logger log = getLogger(MongoDAtlasContainer.class);

    public MongoDAtlasContainer(String mongodbEnterpriseTag) {
        super(DockerImageName
                .parse("mongodb/mongodb-enterprise-server")
                .withTag(mongodbEnterpriseTag)
        );
    }

    MongoDAtlasContainerDef createContainerDef() {
        return new MongoDAtlasContainerDef();
    }
    MongoDAtlasContainerDef getContainerDef() {
        return (MongoDAtlasContainerDef)super.getContainerDef();
    }
    private static class MongoDAtlasContainerDef extends ContainerDef {
        MongoDAtlasContainerDef() {
            this.addExposedTcpPort(27017);
            this.addNetworkAlias("mongod");
            this.setEntrypoint(
                    "/bin/sh",
                    "-c",
                    """
                            echo "%s" > /tmp/keyfile
                            chmod 600 /tmp/keyfile
                            python3 /usr/local/bin/docker-entrypoint.py \\
                              --dbpath "/data/db" \\
                              --keyFile "/tmp/keyfile" \\
                              --replSet "rs-localdev" \\
                              --maxConns 32200 \\
                              --setParameter "mongotHost=mongot:27027" \\
                              --setParameter "searchIndexManagementHostAndPort=mongot:27027" \\
                              --transitionToAuth""".formatted(shared_key_file_content)
            );
            this.setWaitStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 1));
        }
    }

    public String getConnectionString() {
        //Because we are connecting to a single node replica set, we need to use the directConnection=true parameter
        return String.format("mongodb://%s:%d/?directConnection=true", this.getHost(), this.getMappedPort(27017));
    }

    public void waitForMongoTSteadyState() {
        LogMessageWaitStrategy logMessageWaitStrategy = Wait.forLogMessage("(?i).*mongot steady state.*", 1);
        logMessageWaitStrategy.waitUntilReady(this);
    }

    public boolean waitUntilMongoTPingSucceeds(Duration timeout) {
        log.debug("Waiting for mongot to be ready...");
        Instant start = Instant.now();
        while (Instant.now().isBefore(start.plus(timeout))) {
            try {
                log.debug("Pinging mongot...");
                Container.ExecResult execResult = this.execInContainer(this.buildMongoEvalCommand("mongodb://mongot:27027/admin", "db.adminCommand('ping')", "--quiet"));
                if (execResult.getExitCode() == 0) {
                    log.debug("mongot is ready!");
                    return true;
                } else {
                    log.debug("mongot is not ready yet... will retry in 300ms");
                }
            } catch (IOException e) {
                log.warn("An error occurred while waiting for the ping command to succeed against mongot", e);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for the ping command to succeed against mongot", e);
            }
            try {
                sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for the ping command to succeed against mongot", e);
            }
        }
        log.warn("mongot is not ready after {} seconds", timeout.getSeconds());
        return false;
    }

    protected void containerIsStarting(InspectContainerResponse containerInfo) { }

    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        initReplicaSet(reused);
        insertClusterType();
    }

    private String[] buildMongoEvalCommand(String command) {
        return buildMongoEvalCommand(null, command);
    }
    private String[] buildMongoEvalCommand(String host, String command, String... options) {
        StringBuilder mongoShCommandBuilder = new StringBuilder();
        mongoShCommandBuilder.append("mongosh");
        if (options != null) {
            for (String option : options) {
                mongoShCommandBuilder.append(" ").append(option);
            }
        }
        if (host != null) {
            mongoShCommandBuilder.append(" ").append(host);
        }
        mongoShCommandBuilder.append(" --eval");
        mongoShCommandBuilder.append(" \"").append(command).append("\"");

        String mongoShCommand = mongoShCommandBuilder.toString();
        return new String[]{
                "sh",
                "-c",
                mongoShCommand
        };
    }

    /**
     * MongoT requires that MongoD is running as a replica set, as it uses change streams to follow the oplog to update its Lucene indexes.
     */
    private void initReplicaSet(boolean reused) {
        try {
            try {
                if (reused && this.isReplicationSetAlreadyInitialized()) {
                    log.debug("Replica set already initialized.");
                } else {
                    log.debug("Initializing a single node node replica set...");
                    Container.ExecResult execResultInitRs = this.execInContainer(this.buildMongoEvalCommand("rs.initiate();"));
                    log.debug(execResultInitRs.getStdout());
                    if (execResultInitRs.getExitCode() != 0) {
                        String errorMessage = String.format("An error occurred: %s", execResultInitRs.getStdout());
                        log.error(errorMessage);
                        throw new ReplicaSetInitializationException(errorMessage);
                    }

                    log.debug("Awaiting for a single node replica set initialization up to {} attempts", 60);
                    Container.ExecResult execResultWaitForMaster = this.execInContainer(this.buildMongoEvalCommand("""
                            var attempt = 0;
                            while(db.runCommand( { isMaster: 1 } ).ismaster==false) {
                                if (attempt > 60) {
                                    quit(1);
                                }
                                print('An attempt to await for a single node replica set initialization: ' + attempt); 
                                sleep(100);
                                attempt++;
                            }"""
                    ));
                    log.debug(execResultWaitForMaster.getStdout());
                    if (execResultWaitForMaster.getExitCode() != 0) {
                        String errorMessage = String.format("A single node replica set was not initialized in a set timeout: %d attempts", 60);
                        log.error(errorMessage);
                        throw new ReplicaSetInitializationException(errorMessage);
                    }
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isReplicationSetAlreadyInitialized() {
        try {
            Container.ExecResult execCheckRsInit = this.execInContainer(this.buildMongoEvalCommand("if(db.adminCommand({replSetGetStatus: 1})['myState'] != 1) quit(900)"));
            return execCheckRsInit.getExitCode() == 0;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This may be cosmetic, but the Atlas CLI inserts a document indicating the cluster type into this collection
     */
    private void insertClusterType() {
        try {
            ExecResult insertAtlasCliDocument = this.execInContainer(this.buildMongoEvalCommand("mongodb://localhost:27017/admin", "db.atlascli.insertOne({managedClusterType: 'atlasCliLocalDevCluster'});"));
            log.debug(insertAtlasCliDocument.getStdout());
            if (insertAtlasCliDocument.getExitCode() != 0) {
                String errorMessage = String.format("An error occurred: %s", insertAtlasCliDocument.getStdout());
                log.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ReplicaSetInitializationException extends RuntimeException {
        ReplicaSetInitializationException(String errorMessage) {
            super(errorMessage);
        }
    }
}
