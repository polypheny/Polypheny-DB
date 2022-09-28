/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.Collections;
import java.util.HashMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerManager.Container;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.docker.DockerManager.ContainerStatus;

@Category(DockerManagerTest.class)
public class DockerInstanceTest {

    private static ConfigDocker config;
    private static final String imageName = "mongo";


    @BeforeClass
    public static void initClass() {
        if ( Catalog.INSTANCE == null ) {
            // some functionality needs to use the catalog, so we use a mock
            Catalog.setAndGetInstance( new MockCatalogDocker() );
        }
    }


    @Before
    public void initConfig() {
        config = new ConfigDocker( "localhost", null, null, "test" );
        RuntimeConfig.DOCKER_INSTANCES.setList( Collections.singletonList( config ) );
    }


    /**
     * We test if the DockerManager correctly initializes a new container
     */
    @Test
    public void startNotExistsContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerInstance managerLastSession = fakeLastSession( uniqueName, usedPort, true, false );

        //// new session has to handle already running container
        DockerInstance managerThisSession = managerLastSession.generateNewSession( config.id );
        Container restoredContainer = managerThisSession.initialize( new ContainerBuilder( 1, imageName, uniqueName, config.id ).withMappedPort( usedPort, usedPort ).build() );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.getStatus() == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );
    }


    /**
     * Helper method which fakes a previous session, which was terminated and left the container in specified state
     *
     * @param uniqueName the name of the previous container
     * @param usedPort the used port of the previous container
     * @param doDestroy if the container was destroyed previously
     * @param doStop if the container was stopped
     * @return the managerImpl, which is used to fake a new session, but has to use the old client
     */
    private DockerInstance fakeLastSession( String uniqueName, int usedPort, boolean doDestroy, boolean doStop ) {
        // so we can test the initialization process of the DockerManager,
        // when a container is already running
        // we use the impl here
        //// previous session left the container running

        DockerInstance managerLastSession = new DockerInstance( config.id );
        Container container = managerLastSession.initialize( new ContainerBuilder( 1, imageName, uniqueName, config.id ).withMappedPort( usedPort, usedPort ).build() );
        managerLastSession.start( container );
        Catalog.getInstance().addAdapter( "mockedAdapter", "", AdapterType.STORE, new HashMap<>() );

        assert (container.getStatus() == ContainerStatus.RUNNING);
        assert (managerLastSession.getUsedNames().contains( uniqueName ));
        assert (managerLastSession.getUsedPorts().contains( usedPort ));

        if ( doStop ) {
            managerLastSession.stop( container );

            assert (container.getStatus() == ContainerStatus.STOPPED);
            assert (managerLastSession.getUsedNames().contains( uniqueName ));
            assert (managerLastSession.getUsedPorts().contains( usedPort ));
        }

        if ( doDestroy ) {
            managerLastSession.destroy( container );

            assert (container.getStatus() == ContainerStatus.DESTROYED);
            assert (!managerLastSession.getUsedNames().contains( uniqueName ));
            assert (!managerLastSession.getUsedPorts().contains( usedPort ));
        }
        return managerLastSession;
    }


    /**
     * We test if an already running container on system start can correctly be restored
     */
    @Test
    public void runningContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerInstance managerLastSession = fakeLastSession( uniqueName, usedPort, false, false );

        //// new session has to handle already running container
        DockerInstance managerThisSession = managerLastSession.generateNewSession( config.id );
        Container restoredContainer = managerThisSession.initialize( new ContainerBuilder( 1, imageName, uniqueName, config.id ).withMappedPort( usedPort, usedPort ).build() );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.getStatus() == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );
    }


    /**
     * We test if a already existing container, which was stopped can be correctly restored
     */
    @Test
    public void stoppedContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerInstance managerLastSession = fakeLastSession( uniqueName, usedPort, false, true );

        //// new session has to handle already running container
        DockerInstance managerThisSession = managerLastSession.generateNewSession( config.id );
        Container restoredContainer = managerThisSession.initialize( new ContainerBuilder( 1, imageName, uniqueName, config.id ).withMappedPort( usedPort, usedPort ).build() );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.getStatus() == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );
    }

}
