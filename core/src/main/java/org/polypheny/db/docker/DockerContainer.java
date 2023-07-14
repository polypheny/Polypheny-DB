/*
 * Copyright 2019-2023 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.docker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.RuntimeConfig;

/**
 * The container is the main interaction instance for calling classes when interacting with Docker.
 * It holds all information for a specific Container
 */
@Slf4j
public final class DockerContainer {

    private static final Map<String, DockerContainer> containers = new ConcurrentHashMap<>();

    public final String uniqueName;

    /**
     * The UUID of this container.
     */
    @Getter
    private final String containerId;


    public DockerContainer( String containerId, String uniqueName ) {
        this.containerId = containerId;
        this.uniqueName = uniqueName;
        containers.put( containerId, this );
    }


    public static Optional<DockerContainer> getContainerByUUID( String uuid ) {
        return Optional.ofNullable( containers.get( uuid ) );
    }


    private DockerInstance getDockerInstance() {
        return DockerManager.getInstance().getInstanceForContainer( containerId ).get();
    }


    /**
     * Starts the container
     *
     * @return the started container
     */
    public DockerContainer start() throws IOException {
        getDockerInstance().startContainer( this );
        return this;
    }


    /**
     * Stops the container
     */
    public void stop() throws IOException {
        getDockerInstance().stopContainer( this );
    }


    /**
     * Destroys the container, which stops and removes it from the system
     */
    public void destroy() {
        log.info( "Destroying container with ID " + this.getContainerId() );
        getDockerInstance().destroyContainer( this );
        containers.remove( containerId );
    }


    public int execute( List<String> cmd ) throws IOException {
        return getDockerInstance().execute( this, cmd );
    }


    public static String getPhysicalUniqueName( String uniqueName ) {
        // while not all Docker containers belong to an adapter we annotate it anyway
        String name = "polypheny_" + RuntimeConfig.INSTANCE_UUID.getString() + "_" + uniqueName;
        if ( !Catalog.testMode ) {
            return name;
        } else {
            return name + "_test";
        }
    }


    public String getIpAddress() {
        return getDockerInstance().getHost();
    }


    public Optional<Integer> getExposedPort( int port ) {
        try {
            Map<Integer, Integer> portMap = getDockerInstance().getPorts( this );
            return Optional.ofNullable( portMap.getOrDefault( port, null ) );
        } catch ( IOException e ) {
            log.error( "Failed to retrieve portlist for container " + containerId );
            return Optional.empty();
        }
    }


    /**
     * The container gets probed until the defined ready supplier returns true or the timeout is reached
     */
    public boolean waitTillStarted( Supplier<Boolean> isReadySupplier, long maxTimeoutMs ) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        boolean isStarted = isReadySupplier.get();
        while ( !isStarted && (stopWatch.getTime() < maxTimeoutMs) ) {
            try {
                TimeUnit.MILLISECONDS.sleep( 500 );
            } catch ( InterruptedException e ) {
                // ignore
            }
            isStarted = isReadySupplier.get();
        }
        stopWatch.stop();
        return isStarted;
    }

}