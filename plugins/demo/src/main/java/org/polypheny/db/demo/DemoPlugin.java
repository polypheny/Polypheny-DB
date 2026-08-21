/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.demo;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.demo.relational.RelationalStore;
import org.polypheny.db.docker.AutoDocker;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.models.AutoDockerStatus;
import org.polypheny.db.docker.models.DockerInstanceInfo;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class DemoPlugin extends PolyPlugin {
    DockerInstance dockerInstance;

    private final static String NOTEBOOK = "demo.ipynb";

    private TransactionManager transactionManager;

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public DemoPlugin( PluginContext context ) {
        super( context );
    }

    @Override
    public void afterRestoreInit() {
        createNotebook();

        List<DemoStore> demoStores = new ArrayList<>();

        demoStores.add(new DocumentStore( transactionManager, true ));
        demoStores.add(new GraphStore( transactionManager, true ));
        demoStores.add(new RelationalStore( transactionManager, true ));

        Transaction transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, new QueryAnalyzer(), "Demo" );
        Statement statement = transaction.createStatement();

        log.info( "Creating namespaces" );
        demoStores.forEach( store -> store.createNamespace( statement ) );

        log.info( "Creating store adapters" );
        demoStores.forEach( store -> store.createAdapter( this.dockerInstance ) );

        log.info( "Setting up namespaces" );
        demoStores.forEach( store -> store.setupNamespace( statement ) );

        //log.info( "Loading data" );
        //demoStores.get( 2 ).loadData();

        transaction.commit();
    }

    @Override
    public void afterTransactionInit( TransactionManager manager ) {
        this.transactionManager = manager;
    }

    @Override
    public void afterCatalogInit() {
        super.afterCatalogInit();

        log.info( "Auto connecting to docker instance" );
        AutoDocker.getInstance().doAutoConnect();

        AutoDockerStatus status = AutoDocker.getInstance().getStatus();
        if (!status.available()) {
            log.error( "Auto docker instance not available" );
        }
        else if (!status.connected()) {
            log.error( "Auto docker instance not connected" );
        }
        else {
            log.info( "Successfully connected to docker instance" );
        }

        Optional<DockerInstance> dockerInstanceOptional = Optional.empty();
        for ( DockerInstanceInfo dockerInstanceInfo: DockerManager.getInstance().getDockerInstancesMap()) {
            if ( Objects.equals( dockerInstanceInfo.host().hostname(), "localhost" ) ) {
                dockerInstanceOptional = DockerManager.getInstance().getInstanceById( dockerInstanceInfo.id() );
            }
        }

        if (dockerInstanceOptional.isPresent()) {
            this.dockerInstance = dockerInstanceOptional.get();
        }
        else {
            log.error( "Unable to find localhost docker instance" );
        }

    }


    @Override
    public void stop() {
    }

    public void createNotebook() {
        File notebookFile = PolyphenyHomeDirManager.getInstance().registerNewFile( String.format( "data/jupyter/notebooks/%s", NOTEBOOK ) );
        try (InputStream inputStream = DemoPlugin.class.getResourceAsStream( String.format( "/%s", NOTEBOOK ) ) ) {
            if (inputStream == null) {
                throw new FileNotFoundException( String.format( "Unable to find file /%s", NOTEBOOK ) );
            }
            Files.copy(inputStream, notebookFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
        }
        catch ( IOException E ) {
            log.error( E.getMessage() );
        }
    }
}
