package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

public class MongoDAtlasLocalContainer extends GenericContainer<MongoDAtlasLocalContainer> {
    private static final Logger log = getLogger(MongoDAtlasLocalContainer.class);

    public MongoDAtlasLocalContainer() {
        this("latest");
    }
    public MongoDAtlasLocalContainer(String tag) {
        super(DockerImageName
                .parse("mongodb/mongodb-atlas-local")
                .withTag(tag)
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
            this.setWaitStrategy(Wait.forLogMessage(".*mongot steady state.*", 1));
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        //Nasty hack to wait for mongot to be ready - otherwise will get:
        // com.mongodb.MongoCommandException: Command failed with error 125 (CommandFailed): 'Error connecting to Search Index Management service.'
        // if we try to create a search index too soon.
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        super.containerIsStarted(containerInfo, reused);
    }

    public String getConnectionString() {
        //Because we are connecting to a single node replica set, we need to use the directConnection=true parameter
        return String.format("mongodb://%s:%d/?directConnection=true", this.getHost(), this.getMappedPort(27017));
    }
}
