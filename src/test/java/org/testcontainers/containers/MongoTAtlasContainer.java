package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.MongoAtlasConstants.shared_key_file_content;

public class MongoTAtlasContainer extends GenericContainer<MongoTAtlasContainer> {
    private static final Logger log = LoggerFactory.getLogger(MongoTAtlasContainer.class);

    public MongoTAtlasContainer() {
        super(DockerImageName
                .parse("mongodb/mongodb-atlas-search")
                .withTag("preview")
        );
    }

    MongoTAtlasContainerDef createContainerDef() {
        return new MongoTAtlasContainerDef();
    }

    MongoTAtlasContainerDef getContainerDef() {
        return (MongoTAtlasContainerDef)super.getContainerDef();
    }

    protected void containerIsStarting(InspectContainerResponse containerInfo) { }

    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) { }

    private static class MongoTAtlasContainerDef extends ContainerDef {
        MongoTAtlasContainerDef() {
            this.addNetworkAlias("mongot");
            this.setEntrypoint(
                    "/bin/sh",
                    "-c",
                    """
                            echo "%s" > /tmp/keyfile
                            /etc/mongot-localdev/mongot \\
                              --data-dir=/var/lib/mongot \\
                              --keyFile=/tmp/keyfile \\
                              --mongodHostAndPort=mongod:27017""".formatted(shared_key_file_content)
            );
            this.setWaitStrategy(Wait.forLogMessage("(?i).*Starting query server on address 0.0.0.0/0.0.0.0:27027.*", 1));
        }
    }
}
