/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerManager.Container;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.util.Pair;

/**
 * These tests should mainly test the implementation of the
 * DockerManager and its functionality, the functionality of
 * the underlying java-docker library should not be tested
 */
@Category(DockerManagerTest.class)
public class DockerManagerTest {

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
     * We test if the DockerManager exposes the correct names to the callee
     */
    @Test
    public void usedNamesTest() {
        DockerManager manager = DockerManager.getInstance();

        List<String> uniqueNames = Arrays.asList( "test", "test1", "test2" );
        List<Integer> uniquePorts = Arrays.asList( 2302, 2301, 1201 );
        int adapterId = 1;

        Pair.zip( uniqueNames, uniquePorts ).forEach( namePortPairs -> {
            Container container = new ContainerBuilder( adapterId, imageName, namePortPairs.left, config.id )
                    .withMappedPort( namePortPairs.right, namePortPairs.right )
                    .build();
            manager.initialize( container );
        } );
        assert (manager.getUsedNames().containsAll( uniqueNames ));
        assert (manager.getUsedPorts().containsAll( uniquePorts ));

        manager.destroyAll( adapterId );

        assert (!manager.getUsedNames().containsAll( uniqueNames ));
        assert (!manager.getUsedPorts().containsAll( uniquePorts ));

    }


    /**
     * We test if inserting multiple ports is correctly handled
     */
    @Test
    public void usedMultiplePortsTest() {
        DockerManager manager = DockerManager.getInstance();
        int adapterId = 1;

        String uniqueName = "test3";
        List<Integer> multiplePorts = Arrays.asList( 3210, 4929 );
        ContainerBuilder containerBuilder = new ContainerBuilder( adapterId, imageName, uniqueName, config.id );
        multiplePorts.forEach( port -> containerBuilder.withMappedPort( port, port ) );
        manager.initialize( containerBuilder.build() );

        assert (manager.getUsedNames().contains( uniqueName ));
        assert (manager.getUsedPorts().containsAll( multiplePorts ));

        manager.destroyAll( adapterId );

    }


    /**
     * This methods test if the automatic propagation of changes to the responsible
     * RuntimeConfig.DOCKER_INSTANCE get correctly adapted by the {@link DockerManager}
     * and the responsible {@link DockerInstance}
     */
    @Test
    public void changingDockerInstanceTest() {
        DockerManagerImpl manager = (DockerManagerImpl) DockerManager.getInstance();

        String url = "localhost";
        String alias = "name";
        ConfigDocker c = new ConfigDocker( url, null, null, alias );

        // config does not exist for the manager as it was not added to the runtimeConfigs
        assert (!manager.getDockerInstances().containsKey( c.id ));

        RuntimeConfig.DOCKER_INSTANCES.setList( Collections.singletonList( c ) );
        // changes to RuntimeConfig.DOCKER_INSTANCES should automatically been handled by the DockerManger

        assert (manager.getDockerInstances().containsKey( c.id ));

        String newAlias = "name2";
        RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, c.id ).setAlias( newAlias );
        assert (manager.getDockerInstances().get( c.id ).getCurrentConfig().getAlias().equals( newAlias ));

        String newUrl = "localhost2";
        RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, c.id ).setHost( newUrl );
        assert (manager.getDockerInstances().get( c.id ).getCurrentConfig().getHost().equals( newUrl ));

        // when we replace the configs, they corresponding clients should automatically be removed
        ConfigDocker newC = new ConfigDocker( url, null, null, alias );
        RuntimeConfig.DOCKER_INSTANCES.setList( Collections.singletonList( newC ) );

        assert (!manager.getDockerInstances().containsKey( c.id ));
        assert (manager.getDockerInstances().containsKey( newC.id ));


    }

}
